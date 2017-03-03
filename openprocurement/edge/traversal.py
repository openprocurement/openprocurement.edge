# -*- coding: utf-8 -*-
from pyramid.security import (
    ALL_PERMISSIONS,
    Allow,
    Everyone,
)


class Root(object):
    __name__ = None
    __parent__ = None
    __acl__ = [
        (Allow, Everyone, ALL_PERMISSIONS),
    ]

    def __init__(self, request):
        self.request = request
        self.db = request.registry.db


def resource_factory(request):
    root = Root(request)
    return root
