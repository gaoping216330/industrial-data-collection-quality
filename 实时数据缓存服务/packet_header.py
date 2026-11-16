"""Extract only the outer timestamp without constructing point objects."""
import json
import math
import re

SPACE=re.compile(rb'\s*')
STRING=re.compile(rb'"(?:\\.|[^"\\])*"')
TOKEN=re.compile(rb'"(?:\\.|[^"\\])*"|[{}\[\]]')
DELIMITER=re.compile(rb'[,}\]]')

def skip_value(raw,pos):
    pos=SPACE.match(raw,pos).end()
    if pos>=len(raw):raise ValueError('missing JSON value')
    if raw[pos]==34:
        token=STRING.match(raw,pos)
        if token is None:raise ValueError('invalid JSON string')
        return token.end()
    if raw[pos] in (91,123):
        stack=[]
        for token in TOKEN.finditer(raw,pos):
            value=token.group()
            if value.startswith(b'"'):continue
            if value in (b'[',b'{'):stack.append(value)
            else:
                if not stack or (stack.pop(),value) not in ((b'[',b']'),(b'{',b'}')):raise ValueError('invalid JSON nesting')
                if not stack:return token.end()
        raise ValueError('unterminated JSON container')
    match=DELIMITER.search(raw,pos)
    return match.start() if match else len(raw)

def packet_timestamp(raw):
    pos=SPACE.match(raw).end()
    if raw[pos:pos+1]!=b'{':raise ValueError('packet must be an object')
    pos+=1
    while True:
        pos=SPACE.match(raw,pos).end()
        if raw[pos:pos+1]==b'}':raise ValueError('missing outer timestamp')
        key=STRING.match(raw,pos)
        if key is None:raise ValueError('invalid outer JSON key')
        name=json.loads(key.group())
        pos=SPACE.match(raw,key.end()).end()
        if raw[pos:pos+1]!=b':':raise ValueError('missing JSON colon')
        pos=SPACE.match(raw,pos+1).end()
        end=skip_value(raw,pos)
        if name=='timestamp':
            value=json.loads(raw[pos:end])
            if isinstance(value,bool) or not isinstance(value,(int,float,str)):raise ValueError('invalid packet timestamp')
            number=float(value)
            if not math.isfinite(number) or not 0<=number<=4102444800000:raise ValueError('invalid packet timestamp')
            return int(number)
        pos=SPACE.match(raw,end).end()
        if raw[pos:pos+1]!=b',':raise ValueError('missing outer timestamp')
        pos+=1
