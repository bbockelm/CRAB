# This file was created automatically by SWIG.
# Don't modify this file, modify the SWIG interface instead.
# This file is compatible with both classic and new-style classes.

import _CRAB_Server_API

def _swig_setattr(self,class_type,name,value):
    if (name == "this"):
        if isinstance(value, class_type):
            self.__dict__[name] = value.this
            if hasattr(value,"thisown"): self.__dict__["thisown"] = value.thisown
            del value.thisown
            return
    method = class_type.__swig_setmethods__.get(name,None)
    if method: return method(self,value)
    self.__dict__[name] = value

def _swig_getattr(self,class_type,name):
    method = class_type.__swig_getmethods__.get(name,None)
    if method: return method(self)
    raise AttributeError,name

import types
try:
    _object = types.ObjectType
    _newclass = 1
except AttributeError:
    class _object : pass
    _newclass = 0
del types


class CRAB_Server_Session(_object):
    __swig_setmethods__ = {}
    __setattr__ = lambda self, name, value: _swig_setattr(self, CRAB_Server_Session, name, value)
    __swig_getmethods__ = {}
    __getattr__ = lambda self, name: _swig_getattr(self, CRAB_Server_Session, name)
    def __repr__(self):
        return "<C CRAB_Server_Session instance at %s>" % (self.this,)
    def __init__(self, *args):
        _swig_setattr(self, CRAB_Server_Session, 'this', _CRAB_Server_API.new_CRAB_Server_Session(*args))
        _swig_setattr(self, CRAB_Server_Session, 'thisown', 1)
    def __del__(self, destroy=_CRAB_Server_API.delete_CRAB_Server_Session):
        try:
            if self.thisown: destroy(self)
        except: pass
    def transferTaskAndSubmit(*args): return _CRAB_Server_API.CRAB_Server_Session_transferTaskAndSubmit(*args)
    def sendCommand(*args): return _CRAB_Server_API.CRAB_Server_Session_sendCommand(*args)
    def getTaskStatus(*args): return _CRAB_Server_API.CRAB_Server_Session_getTaskStatus(*args)

class CRAB_Server_SessionPtr(CRAB_Server_Session):
    def __init__(self, this):
        _swig_setattr(self, CRAB_Server_Session, 'this', this)
        if not hasattr(self,"thisown"): _swig_setattr(self, CRAB_Server_Session, 'thisown', 0)
        _swig_setattr(self, CRAB_Server_Session,self.__class__,CRAB_Server_Session)
_CRAB_Server_API.CRAB_Server_Session_swigregister(CRAB_Server_SessionPtr)


