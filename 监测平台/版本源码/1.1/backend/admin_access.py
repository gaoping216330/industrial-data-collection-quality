"""Local administrator sessions; credentials never served as static assets."""
import json
import secrets
import threading
import time
from datetime import timedelta, datetime
from flask import jsonify, request, session
from werkzeug.security import generate_password_hash, check_password_hash


def install_admin(app, root):
    path=root/'admin_credentials.json'
    if not path.exists():
        password=secrets.token_urlsafe(15)
        path.write_text(json.dumps({'username':'admin','password_hash':generate_password_hash(password),'secret_key':secrets.token_hex(32)},indent=2),encoding='utf-8')
        (root/'管理员初始登录.txt').write_text('管理员用户名：admin\n初始密码：'+password+'\n请在网页右上角登录；登录后可修改密码。请妥善保管并删除此初始凭据文件。\n',encoding='utf-8')
    settings=json.loads(path.read_text(encoding='utf-8'))
    app.secret_key=settings['secret_key']
    app.config.update(SESSION_COOKIE_HTTPONLY=True,SESSION_COOKIE_SAMESITE='Strict',PERMANENT_SESSION_LIFETIME=timedelta(hours=8))
    attempts={};lock=threading.Lock()

    settings.setdefault('users',{})
    settings.setdefault('version',0)
    def account(username):
        if username==settings['username']:
            return dict(username=username,name='超级管理员',password_hash=settings['password_hash'],enabled=True,version=settings.get('version',0),role='superadmin')
        user=settings['users'].get(username)
        return dict(user,username=username,role='admin') if user else None
    def persist(action,target,details):
        callback=app.config.get('AUDIT_ACCOUNT_CHANGE');audit_id=None
        temporary=path.with_suffix('.tmp')
        try:
            audit_id=callback(action,target,details,'REQUESTED') if callback else None
            temporary.write_text(json.dumps(settings,ensure_ascii=False,indent=2),encoding='utf-8');temporary.replace(path)
        except Exception:
            # Reload the last durable settings if the atomic replacement failed.
            durable=json.loads(path.read_text(encoding='utf-8'));durable.setdefault('users',{});durable.setdefault('version',0);settings.clear();settings.update(durable)
            if callback and audit_id:callback(action,target,details,'FAILED',audit_id)
            raise
        if callback:callback(action,target,details,'SUCCESS',audit_id)
    def public(user):return {k:user.get(k) for k in ('username','name','role','enabled','created_at','created_by','updated_at')}
    def superuser():return session.get('admin') and session.get('role')=='superadmin'
    def audit_rejected(note):
        audit=app.config.get('AUDIT_ALARM_REJECTION')
        if audit and request.path.startswith('/api/points/') and request.path.endswith('/alarm'):
            try:audit('DENIED',note)
            except Exception:app.logger.exception('Failed to audit denied alarm operation')

    @app.before_request
    def protect_mutations():
        if session.get('admin'):
            user=account(session.get('username',settings['username']))
            if not user or not user['enabled'] or session.get('account_version',0)!=user.get('version',0):session.clear()
            else:session['username']=user['username'];session['role']=user['role'];session['display_name']=user.get('name','')
        if request.method not in ('POST','PUT','PATCH','DELETE'):return
        if request.headers.get('Sec-Fetch-Site')=='cross-site' or (request.headers.get('Origin') and request.headers['Origin']!=request.host_url.rstrip('/')):
            audit_rejected('跨站请求被拒绝');return jsonify(error='不允许跨站修改'),403
        if not request.is_json:
            audit_rejected('非JSON修改请求被拒绝');return jsonify(error='请使用JSON请求'),415
        # Read-only chart export uses POST for its sample payload, without changing settings.
        if request.path=='/api/trend-export':return
        if request.path!='/api/admin/login' and not session.get('admin'):
            audit_rejected('未登录管理员，操作被拒绝');return jsonify(error='请先登录管理员账户'),401

    @app.get('/api/admin/status')
    def admin_status():return jsonify(admin=bool(session.get('admin')),username=session.get('username'),name=session.get('display_name'),role=session.get('role'),superadmin=superuser())

    @app.post('/api/admin/login')
    def admin_login():
        ip=request.remote_addr;now=time.monotonic()
        with lock:
            recent=[t for t in attempts.get(ip,[]) if now-t<300]
            if len(recent)>=10:return jsonify(error='尝试次数过多，请5分钟后重试'),429
            attempts[ip]=recent+[now]
        data=request.get_json(silent=True) or {};user=account(str(data.get('username','')).strip())
        if not user or not user['enabled'] or not check_password_hash(user['password_hash'],str(data.get('password',''))):return jsonify(error='工号或密码错误，或账号已停用'),401
        with lock:attempts.pop(ip,None)
        session.clear();session.update(admin=True,username=user['username'],role=user['role'],display_name=user.get('name',''),account_version=user.get('version',0),login_at=datetime.now().isoformat(timespec='seconds'));session.permanent=True
        return jsonify(success=True,role=user['role'])

    @app.post('/api/admin/logout')
    def admin_logout():session.clear();return jsonify(success=True)

    @app.post('/api/admin/password')
    def admin_password():
        data=request.get_json(silent=True) or {};username=session['username'];user=account(username)
        if not check_password_hash(user['password_hash'],str(data.get('old_password',''))):return jsonify(error='原密码错误'),400
        password=str(data.get('password',''))
        if not password:return jsonify(error='请输入新密码'),400
        with lock:
            target=settings if username==settings['username'] else settings['users'][username]
            target['password_hash']=generate_password_hash(password);target['version']=target.get('version',0)+1
            persist('CHANGE_PASSWORD',username,{'note':'本人修改密码'})
        session.clear();return jsonify(success=True)

    @app.get('/api/admin/users')
    def admin_users():
        if not superuser():return jsonify(error='仅超级管理员可以管理人员'),403
        return jsonify(items=[public(account(settings['username']))]+[public(account(k)) for k in sorted(settings['users'])])

    @app.post('/api/admin/users')
    def create_admin_user():
        if not superuser():return jsonify(error='仅超级管理员可以新增人员'),403
        import re
        data=request.get_json(silent=True) or {};username=str(data.get('username','')).strip();name=str(data.get('name','')).strip();password=str(data.get('password',''))
        if not re.fullmatch(r'[0-9]{1,20}',username):return jsonify(error='工号必须为1至20位数字'),400
        if not name or len(name)>64:return jsonify(error='请填写姓名（最多64字）'),400
        if not password:return jsonify(error='请输入初始密码'),400
        if data.get('role','admin')!='admin':return jsonify(error='只能创建普通管理员'),400
        with lock:
            if account(username):return jsonify(error='该工号已存在'),409
            stamp=datetime.now().isoformat(timespec='seconds')
            settings['users'][username]=dict(name=name,password_hash=generate_password_hash(password),enabled=True,version=0,created_at=stamp,updated_at=stamp,created_by=session['username'])
            persist('CREATE_USER',username,{'name':name,'role':'admin'})
        return jsonify(success=True)

    @app.put('/api/admin/users/<username>')
    def update_admin_user(username):
        if not superuser():return jsonify(error='仅超级管理员可以管理人员'),403
        if username==settings['username']:return jsonify(error='超级管理员不能通过人员管理被停用或重置，请使用修改本人密码'),400
        data=request.get_json(silent=True) or {}
        if set(data)-{'name','enabled','password'}:return jsonify(error='不支持该修改字段'),400
        if 'enabled' in data and type(data['enabled']) is not bool:return jsonify(error='enabled必须为布尔值'),400
        if 'password' in data and (not isinstance(data['password'],str) or not data['password']):return jsonify(error='请输入新密码'),400
        if 'name' in data and (not isinstance(data['name'],str) or not data['name'].strip() or len(data['name'])>64):return jsonify(error='姓名不合法'),400
        with lock:
            user=settings['users'].get(username)
            if not user:return jsonify(error='工号不存在'),404
            details={'old_name':user['name'],'old_enabled':user['enabled']}
            if 'name' in data:user['name']=data['name'].strip()
            if 'enabled' in data:user['enabled']=data['enabled']
            if 'password' in data:user['password_hash']=generate_password_hash(data['password'])
            user['version']=user.get('version',0)+1;user['updated_at']=datetime.now().isoformat(timespec='seconds')
            details.update(name=user['name'],enabled=user['enabled'],password_reset='password' in data)
            persist('UPDATE_USER',username,details)
        return jsonify(success=True)
