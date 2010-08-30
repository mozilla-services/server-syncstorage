#!/usr/bin/python

# Implementation of a Weave client

import urllib
import urllib2
import httplib
import hashlib
import logging
import unittest
import base64
import json

opener = urllib2.build_opener(urllib2.HTTPHandler)

class WeaveException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

def createUser(serverURL, userID, password, email, secret = None, captchaChallenge = None, captchaResponse = None, withHost =None):
    if userID.find('"') >=0:
        raise ValueError("Weave userIDs may not contain the quote character")
    if email.find('"') >=0:
        raise ValueError("Weave email addresses may not contain the quote character")
    if secret and secret.find('"') >=0:
        raise ValueError("Weave secret may not contain the quote character")

    url = serverURL + "/user/1/%s/" % userID

    secretStr = ""
    captchaStr = ""
    if secret:
        secretStr = ''', "secret":"%s"''' % secret

    if captchaChallenge and captchaResponse:
        if secret:
            raise WeaveException("Cannot provide both a secret and a captchaResponse to createUser")
        captchaStr = ''', "captcha-challenge":"%s", "captcha-response":"%s"''' % (captchaChallenge, captchaResponse)

    payload = '''{"password":"%s", "email": "%s"%s%s}''' % (password, email, secretStr, captchaStr)

    req = urllib2.Request(url, data=payload)
    if withHost:
        req.add_header("Host", withHost)

    req.get_method = lambda: 'PUT'
    try:
        f = opener.open(req)
        result = f.read()
        if result != userID:
            raise WeaveException("Unable to create new user: got return value '%s' from server" % result)

    except urllib2.URLError, e:
        msg = ""
        try:
            msg = e.read()
        except:
            pass
        raise WeaveException("Unable to communicate with Weave server: " + str(e) + "; %s" % msg)


def checkNameAvailable(serverURL, userID, withHost=None):
    if userID.find('"') >=0:
        raise ValueError("Weave userIDs may not contain the quote character")

    url = serverURL + "/user/1/%s/" % userID

    req = urllib2.Request(url)
    if withHost:
        req.add_header("Host", withHost)
    try:
        f = urllib2.urlopen(req)
        result = f.read()
        if result == "1":
            return False
        elif result == "0":
            return True
        else:
            raise WeaveException("Unexpected return value from server on name-availability request: '%s'" % result)
    except urllib2.URLError, e:
        raise WeaveException("Unable to communicate with Weave server: " + str(e))


def getUserStorageNode(serverURL, userID, password, withHost=None):
    if userID.find('"') >=0:
        raise ValueError("Weave userIDs may not contain the quote character")

    url = serverURL + "/user/1/%s/node/weave" % userID


    req = urllib2.Request(url)
    base64string = base64.encodestring('%s:%s' % (userID, password))[:-1]
    req.add_header("Authorization", "Basic %s" % base64string)
    if withHost:
        req.add_header("Host", withHost)

    try:
        f = opener.open(req)
        result = f.read()
        f.close()
        return result

    except urllib2.URLError, e:
        if str(e).find("404") >= 0:
            return serverURL
        else:
            raise WeaveException("Unable to communicate with Weave server: " + str(e))


def changeUserEmail(serverURL, userID, password, newemail, withHost=None):
    if userID.find('"') >=0:
        raise ValueError("Weave userIDs may not contain the quote character")
    if newemail.find('"') >=0:
        raise ValueError("Weave email addresses may not contain the quote character")

    url = serverURL + "/user/1/%s/email" % userID

    payload = newemail

    req = urllib2.Request(url, data=payload)
    base64string = base64.encodestring('%s:%s' % (userID, password))[:-1]
    req.add_header("Authorization", "Basic %s" % base64string)
    if withHost:
        req.add_header("Host", withHost)
    req.get_method = lambda: 'POST'
    try:
        f = opener.open(req)
        result = f.read()
        if result != newemail:
            raise WeaveException("Unable to change user email: got return value '%s' from server" % result)

    except urllib2.URLError, e:
        raise WeaveException("Unable to communicate with Weave server: %s" % e)



def changeUserPassword(serverURL, userID, password, newpassword, withHost=None):
    if userID.find('"') >=0:
        raise ValueError("Weave userIDs may not contain the quote character")

    url = serverURL + "/user/1/%s/password" % userID

    payload = newpassword
    req = urllib2.Request(url, data=payload)
    base64string = base64.encodestring('%s:%s' % (userID, password))[:-1]
    req.add_header("Authorization", "Basic %s" % base64string)
    if withHost:
        req.add_header("Host", withHost)
    req.get_method = lambda: 'POST'
    try:

        f = opener.open(req)
        result = f.read()
        if result != "success":
            raise WeaveException("Unable to change user password: got return value '%s' from server" % result)

    except urllib2.URLError, e:
        raise WeaveException("Unable to communicate with Weave server: %s" % e)



def deleteUser(serverURL, userID, password, withHost=None):
    if userID.find('"') >=0:
        raise ValueError("Weave userIDs may not contain the quote character")

    url = serverURL + "/user/1/%s/" % userID

    req = urllib2.Request(url)
    base64string = base64.encodestring('%s:%s' % (userID, password))[:-1]
    req.add_header("Authorization", "Basic %s" % base64string)
    if withHost:
        req.add_header("Host", withHost)
    req.get_method = lambda: 'DELETE'
    try:
        f = opener.open(req)
        result = f.read()

    except urllib2.URLError, e:
        msg = ""
        try:
            msg = e.read()
        except:
            pass
        raise WeaveException("Unable to communicate with Weave server: " + str(e) + "; %s" % msg)



def setUserProfile(serverURL, userID, profileField, profileValue, withHost=None):
    if userID.find('"') >=0:
        raise ValueError("Weave userIDs may not contain the quote character")

    url = serverURL + "/user/1/%s/profile" % userID

    payload = newpassword
    req = urllib2.Request(url, data=payload)
    base64string = base64.encodestring('%s:%s' % (userID, password))[:-1]
    req.add_header("Authorization", "Basic %s" % base64string)
    req.get_method = lambda: 'POST'
    if withHost:
        req.add_header("Host", withHost)
    try:
        f = opener.open(req)
        result = f.read()
        if result != "success":
            raise WeaveException("Unable to change user password: got return value '%s' from server" % result)

    except urllib2.URLError, e:
        raise WeaveException("Unable to communicate with Weave server: %s" % e)





def storage_http_op(method, userID, password, url, payload=None, asJSON=True, ifUnmodifiedSince=None, withConfirmation=None, withAuth=True, withHost=None, outputFormat=None):

    req = urllib2.Request(url, data=payload)
    if withAuth:
        base64string = base64.encodestring('%s:%s' % (userID, password))[:-1]
        req.add_header("Authorization", "Basic %s" % base64string)
    if ifUnmodifiedSince:
        req.add_header("X-If-Unmodified-Since", "%s" % ifUnmodifiedSince)
    if withConfirmation:
        req.add_header("X-Confirm-Delete", "true")
    if outputFormat:
        req.add_header("Accept", outputFormat)
    if withHost:
        req.add_header("Host", withHost)

    req.get_method = lambda: method

    #print "%s %s" % (method, url)
    #if payload: print "> %s" % payload
    try:
        f = opener.open(req)
        result = f.read()
        #print "< %s" % result
        if asJSON:
            return json.loads(result)
        else:
            return result
    except urllib2.URLError, e:
        msg = ""
        try:
            msg = e.read()
        except:
            pass
        # TODO process error code
        raise WeaveException("Unable to communicate with Weave server: %s" % e)


def add_or_modify_item(storageServerURL, userID, password, collection, item, urlID=None, ifUnmodifiedSince=None, withHost=None):
    '''Adds the WBO defined in 'item' to 'collection'.  If the WBO does
    not contain a payload, will update the provided metadata fields on an
    already-defined object.

    Returns the timestamp of the modification.'''
    if urlID:
        url = storageServerURL + "/1.0/%s/storage/%s/%s" % (userID, collection, urllib.quote(urlID))
    else:
        url = storageServerURL + "/1.0/%s/storage/%s" % (userID, collection)
    if type(item) == str:
        itemJSON = item
    else:
        itemJSON = json.dumps(item)
    return storage_http_op("PUT", userID, password, url, itemJSON, asJSON=False, ifUnmodifiedSince=ifUnmodifiedSince, withHost=withHost)

def add_or_modify_items(storageServerURL, userID, password, collection, itemArray, ifUnmodifiedSince=None, withHost=None):
    '''Adds all the items defined in 'itemArray' to 'collection'; effectively
    performs an add_or_modifiy_item for each.

    Returns a map of successful and modified saves, like this:

    {"modified":1233702554.25,
     "success":["{GXS58IDC}12","{GXS58IDC}13","{GXS58IDC}15","{GXS58IDC}16","{GXS58IDC}18","{GXS58IDC}19"],
     "failed":{"{GXS58IDC}11":["invalid parentid"],
                         "{GXS58IDC}14":["invalid parentid"],
                         "{GXS58IDC}17":["invalid parentid"],
                         "{GXS58IDC}20":["invalid parentid"]}
    }
    '''
    url = storageServerURL + "/1.0/%s/storage/%s" % (userID, collection)
    if type(itemArray) == str:
        itemArrayJSON = itemArray
    else:
        itemArrayJSON = json.dumps(itemArray)
    return storage_http_op("POST", userID, password, url, itemArrayJSON, ifUnmodifiedSince=ifUnmodifiedSince, withHost=withHost)


def delete_item(storageServerURL, userID, password, collection, id, ifUnmodifiedSince=None, withHost=None):
    url = storageServerURL + "/1.0/%s/storage/%s/%s" % (userID, collection, urllib.quote(id))
    return storage_http_op("DELETE", userID, password, url, ifUnmodifiedSince=ifUnmodifiedSince, withHost=withHost)

def delete_items(storageServerURL, userID, password, collection, idArray=None, params=None, withHost=None):
    if params:
        if idArray:
            url = storageServerURL + "/1.0/%s/storage/%s?ids=%s&%s" % (userID, collection, urllib.quote(','.join(idArray)), params)
        else:
            url = storageServerURL + "/1.0/%s/storage/%s?%s" % (userID, collection, params)
    else:
        if idArray:
            url = storageServerURL + "/1.0/%s/storage/%s?ids=%s" % (userID, collection, urllib.quote(','.join(idArray)))
        else:
            url = storageServerURL + "/1.0/%s/storage/%s" % (userID, collection)
    return storage_http_op("DELETE", userID, password, url, withHost=withHost)

def delete_items_older_than(storageServerURL, userID, password, collection, timestamp, withHost=None):
    url = storageServerURL + "/1.0/%s/storage/%s?older=%s" % (userID, collection, timestamp)
    return storage_http_op("DELETE", userID, password, url, withHost=withHost)

def delete_all(storageServerURL, userID, password, confirm=True, withHost=None):
    '''The only reason you'd want confirm=False is for unit testing'''
    url = storageServerURL + "/1.0/%s/storage" % (userID)
    return storage_http_op("DELETE", userID, password, url, asJSON=False, withConfirmation=confirm, withHost=withHost)

def get_collection_counts(storageServerURL, userID, password, withHost=None):
    url = storageServerURL + "/1.0/%s/info/collection_counts" % (userID)
    return storage_http_op("GET", userID, password, url, withHost=withHost)

def get_collection_timestamps(storageServerURL, userID, password, withHost=None):
    url = storageServerURL + "/1.0/%s/info/collections" % (userID)
    return storage_http_op("GET", userID, password, url, withHost=withHost)

def get_collection_ids(storageServerURL, userID, password, collection, params=None, asJSON=True, outputFormat=None, withHost=None):
    if params:
        url = storageServerURL + "/1.0/%s/storage/%s?%s" % (userID, collection, params)
    else:
        url = storageServerURL + "/1.0/%s/storage/%s" % (userID, collection)
    return storage_http_op("GET", userID, password, url, asJSON=asJSON, outputFormat=outputFormat, withHost=withHost)

def get_item(storageServerURL, userID, password, collection, id, asJSON=True, withAuthUser=None, withAuth=True, withHost=None):
    """withAuth is used for testing only: if set to False the Authorization header is omitted.
     withAuthUser is used for testing only: it sets the HTTP Authorize user to something other than userID"""
    url = storageServerURL + "/1.0/%s/storage/%s/%s?full=1" % (userID, collection, urllib.quote(id, safe=''))
    authUser = userID
    if withAuthUser: authUser = withAuthUser
    return storage_http_op("GET", authUser, password, url, asJSON=asJSON, withAuth=withAuth, withHost=withHost)

def get_quota(storageServerURL, userID, password, withHost=None):
    "Returns an array of [<amount used>,<limit>]"
    url = storageServerURL + "/1.0/%s/info/quota" % (userID)
    return storage_http_op("GET", userID, password, url, withHost=withHost)
