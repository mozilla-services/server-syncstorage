#!/usr/bin/python

# Tests that exercise the basic functions of the user and sync server.

# Implementations of well-known or specified protocols are contained elsewhere.
# These tests only exercise those functions that are specific to the
# implementation of the server.

import random
import base64
import logging
import urllib2
import httplib
import hashlib
import unittest
import time
from base64 import b64encode
opener = urllib2.build_opener(urllib2.HTTPHandler)

import weave

# Import configuration
import test_config
SERVER_BASE = test_config.SERVER_BASE

class TestAccountManagement(unittest.TestCase):

	def setUp(self):
		self.personBase = 'weaveunittest_' + ''.join([chr(random.randint(ord('a'), ord('z'))) for i in xrange(10)])

	def testAccountManagement(self):
		email = 'testuser@test.com'
		password = 'mypassword'
		userID = self.personBase

		# Name should be available
		self.failUnless(weave.checkNameAvailable(SERVER_BASE, userID))

		# Create a user
#		weave.createUser(SERVER_BASE, userID, password, email, secret='seekrit')
#		weave.createUser(SERVER_BASE, userID, password, email, 
#			captchaChallenge='027JBqJ88ZMIFz8ncEifm0HnibtyvB',
#			captchaResponse='of truant')
		weave.createUser(SERVER_BASE, userID, password, email)

		# NOTE that we currently have no way of testing that email address persisted correctly

		# Name should be unavailable
		self.failIf(weave.checkNameAvailable(SERVER_BASE, userID))

		# Storage node
		storageNode = weave.getUserStorageNode(SERVER_BASE, userID, password)
		self.failIf(storageNode == None)

		# With wrong password 
		# Actually, no password is required for the storage node right now
#		try:
#			storageNode = weave.getUserStorageNode(SERVER_BASE, userID, "wrongPassword")
#			self.fail("Should have failed to get storage node with wrong password")
#		except weave.WeaveException:
#			pass
	
		# Change the email address
		newEmail = 'changed@test.com'
		weave.changeUserEmail(SERVER_BASE, userID, password, newEmail)

		# With wrong password
		try:
			weave.changeUserEmail(SERVER_BASE, userID, "wrongPassword", "shouldnotwork@test.com")
			self.fail("Should have failed to change email with wrong password")
		except weave.WeaveException:
			pass

		# Change the password
		newPassword = 'mynewpassword'
		weave.changeUserPassword(SERVER_BASE, userID, password, newPassword)

		# This doesn't actually check anything because we don't use the password for getUserStorageNode
		storageNode = weave.getUserStorageNode(SERVER_BASE, userID, newPassword)
		self.failIf(storageNode == None)

		# TODO Exercise Weave-Password-Reset feature

		# With wrong password
		try:
			weave.changeUserPassword(SERVER_BASE, userID, "shouldnotwork", newPassword)
			self.fail("Should have failed to change password with wrong password")
		except weave.WeaveException:
			pass
		
		# Can I change my email?  This proves that the password change worked.
		anotherNewEmail = 'changedagain@test.com'
		weave.changeUserEmail(SERVER_BASE, userID, newPassword, anotherNewEmail)
		
		# Delete with wrong password
		try:
			weave.deleteUser(SERVER_BASE, userID, "wrongPassword")
			self.fail("Should have failed to delete user with wrong password")
		except weave.WeaveException:
			pass

		# Delete
		weave.deleteUser(SERVER_BASE, userID, newPassword)
		self.failUnless(weave.checkNameAvailable(SERVER_BASE, userID))
		try:
			storageNode = weave.getUserStorageNode(SERVER_BASE, userID, newPassword)
			self.fail("Should have failed to get user storage node after delete")
		except weave.WeaveException:
			pass


	# TODO: Test UTF-8 encoded names; they should work

	def testBoundaryCases(self):
		# Bad usernames
		for i in (range(ord(' ')+1, ord('0')-1) + 
							range(ord('9')+1, ord('A')-1) + 
							range(ord('Z')+1, ord('a')-1) +
							range(ord('z')+1, 127)):
			if chr(i) == '"': continue
			if chr(i) == '-': continue
			if chr(i) == '.': continue
			if chr(i) == '_': continue
			if chr(i) == '#': continue # technically this is okay, since the name just gets split at the '#'
			if chr(i) == '/': continue # technically this is okay, since the name just gets split at the '/'
			if chr(i) == '?': continue # technically this is okay, since the name just gets split at the '?'
			
			try:
				self.failIf(weave.checkNameAvailable(SERVER_BASE, "badcharactertest" + chr(i)),
					"checkNameAvailable should return error for name containing '%s' character" % chr(i))
			except weave.WeaveException, e:
				pass

			try:
				self.failIf(weave.createUser(SERVER_BASE, "badcharactertest" + chr(i), "ignore", "ignore"),
					"createUser should throw error for name containing '%s' character" % chr(i))
				self.fail("Should have failed with bad username")
			except weave.WeaveException, e:
				pass

		try:
			veryLongName = "VeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryLongUsername"
			weave.createUser(SERVER_BASE, veryLongName, "password", "ignore")
			self.fail("Should have failed with bad (too long) user name")
		except:
			pass

class TestStorage(unittest.TestCase):

	def setUp(self):
		self.password = 'mypassword'
		self.email = 'testuser@test.com'
		
	def failUnlessObjsEqualWithDrift(self, o1, o2):
		"Helper function to compare two maps; the 'modified' field is compared with almostEqual"
		for i in o1.items():
			if not i[0] in o2:
				self.fail("%s != %s" % (str(o1), str(o2)))
			if i[0] == "modified":
				self.failUnlessAlmostEqual(float(i[1]), float(o2['modified']))
			else:
				if o1 != o2:
					self.fail("%s != %s" % (str(o1), str(o2)))
		for i in o2.keys():
			if not i in o1:
				self.fail("%s != %s" % (str(o1), str(o2)))
		

	def createCaseUser(self):
		"Helper function to create a new user; returns the userid and storageServer node"
		userID = 'weaveunittest_' + ''.join([chr(random.randint(ord('a'), ord('z'))) for i in xrange(10)])
		self.failUnless(weave.checkNameAvailable(SERVER_BASE, userID))
		weave.createUser(SERVER_BASE, userID, self.password, self.email)
		storageServer = weave.getUserStorageNode(SERVER_BASE, userID, self.password)
		return (userID, storageServer)

	def testAdd(self):
		"testAdd: An object can be created with all optional parameters, and everything persists correctly."
		userID, storageServer = self.createCaseUser()
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'abcd1234', 'sortindex':3, 'parentid':'dearolddad', 'predecessorid':'bigbrother', 'payload':'ThisIsThePayload'})
		result = weave.get_item(storageServer, userID, self.password, 'coll', 'abcd1234')
		self.failUnlessObjsEqualWithDrift(result, {'id':'abcd1234', 'payload':'ThisIsThePayload', 'modified':float(ts), 'sortindex':3, 'parentid':'dearolddad', 'predecessorid':'bigbrother'})

	def testAdd_IDFromURL(self):
		"testAdd_IDFromURL: An object can be created with an ID from the URL, with no ID in the provided payload"
		userID, storageServer = self.createCaseUser()
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'payload':'ThisIsThePayload'}, urlID='thisIsMyID')
		result = weave.get_item(storageServer, userID, self.password, 'coll', 'thisIsMyID')
		self.failUnlessObjsEqualWithDrift(result, {'id':'thisIsMyID', 'payload':'ThisIsThePayload', 'modified':float(ts)})

	def testAdd_IDFromURL_UnusualCharacters(self):
		"testAdd_IDFromURL: An object can be created with an ID from the URL, with no ID in the provided payload, with unusual characters"
		userID, storageServer = self.createCaseUser()
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'payload':'ThisIsThePayload'}, urlID='a/b?c#d~f')
		result = weave.get_item(storageServer, userID, self.password, 'coll', 'a/b?c#d~f')
		self.failUnlessObjsEqualWithDrift(result, {'id':'a/b?c#d~f', 'payload':'ThisIsThePayload', 'modified':float(ts)})

	def testAdd_SlashID(self):
		"testAdd_SlashID: An object can be created with slashes in the ID, and subsequently retrieved."
		userID, storageServer = self.createCaseUser()
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'payload':'ThisIsThePayload', 'id':'abc/def'})
		result = weave.get_item(storageServer, userID, self.password, 'coll', 'abc/def')
		self.failUnlessObjsEqualWithDrift(result, {'id':'abc/def', 'payload':'ThisIsThePayload', 'modified':float(ts)})

	def testAdd_IfUnmodifiedSince_NotModified(self):
		"testAdd_IfUnmodifiedSince_NotModified: If an IfUnmodifiedSince header is provided, and the collection has not been changed, an attempt succeeds."
		userID, storageServer = self.createCaseUser()
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'payload':'ThisIsThePayload'})
		ts2 = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'payload':'ThisIsThePayload'}, ifUnmodifiedSince=ts)
		result = weave.get_item(storageServer, userID, self.password, 'coll', '1234')
		self.failUnlessObjsEqualWithDrift(result, {'id':'1234', 'payload':'ThisIsThePayload', 'modified':float(ts2)})

	def testAdd_IfUnmodifiedSince_Modified(self):
		"testAdd_IfUnmodifiedSince_Modified: If an IfUnmodifiedSince header is provided, and the collection has changed, the attempt fails."
		userID, storageServer = self.createCaseUser()
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'payload':'ThisIsThePayload'})
		ts2 = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'payload':'ThisIsThePayload'})
		try:
			ts3 = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'payload':'ThisIsThePayload'}, ifUnmodifiedSince=ts)
			self.fail("Attempt to add an item when the collection had changed after the ifModifiedSince time should have failed")
		except weave.WeaveException, e:
			self.failUnless(str(e).find("HTTP Error 412: Precondition Failed") > 0, "Should have been an HTTP 412 error")

	def testAdd_NoIDOrPayload(self):
		"testAdd_NoIDOrPayload: Attempts to create an object with no ID or payload do not work."
		# Empty payload is fine
		userID, storageServer = self.createCaseUser()
		try:
			ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {})
			self.fail("Attempt to add an item when the collection had changed after the ifModifiedSince time should have failed")
		except weave.WeaveException, e:
			self.failUnless(str(e).find("HTTP Error 400: Bad Request") > 0, "Should have been an HTTP 400 error")

	def testAdd_EmptyPayload(self):
		"testAdd_EmptyPayload: Attempts to create an object with a zero-length payload work correctly."
		# Empty payload is fine
		userID, storageServer = self.createCaseUser()
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'abcd1234', 'payload':''})
		result = weave.get_item(storageServer, userID, self.password, 'coll', 'abcd1234')
		self.failUnlessObjsEqualWithDrift(result, {'id':'abcd1234', 'payload':'', 'modified':float(ts)})

	def testAdd_EmptyCollection(self):
		"testAdd_EmptyCollection: Attempts to create an object without a collection report an error"
		userID, storageServer = self.createCaseUser()
		try:
			ts = weave.add_or_modify_item(storageServer, userID, self.password, '', {'id':'1234','payload':'ThisIsThePayload'})
			self.fail("Should have reported error with zero-length collection")
		except weave.WeaveException, e:
			self.failUnless(str(e).find("HTTP Error 400: Bad Request") > 0, "Should have been an HTTP 400 error")

	def testAdd_MissingID(self):
		"testAdd_MissingID: Attempts to create an object without an ID report an error"
		userID, storageServer = self.createCaseUser()
		try:
			ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'payload':'ThisIsThePayload'})
			self.fail("Should have reported error with missing ID")
		except weave.WeaveException, e:
			self.failUnless(str(e).find("HTTP Error 400: Bad Request") > 0, "Should have been an HTTP 400 error")
			
	def testAdd_NullIDCharacter(self):
		"testAdd_NullIDCharacter: Null bytes are legal in objectIDs, and objects can be retrieved using them"
		userID, storageServer = self.createCaseUser()
		id = '123\\0000123'
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':id, 'payload':'ThisIsThePayload'})
		result = weave.get_item(storageServer, userID, self.password, 'coll', id)
		self.failUnlessObjsEqualWithDrift(result, {'id':'123\\0000123', 'payload':'ThisIsThePayload', 'modified':float(ts)})

	# There are no forbidden characters in an ID right now - VARBINARY
	def skiptestAdd_UnusualIDCharacters(self):
		"testAdd_UnusualIDCharacters: All bytes values from 01 to 255 are legal in an object ID"
		userID, storageServer = self.createCaseUser()
		for i in range(1,256):
			id = '123\\00' + chr(i).encode("hex")
			ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':id, 'payload':'ThisIsThePayload'})
			result = weave.get_item(storageServer, userID, self.password, 'coll', id)
			self.failUnlessObjsEqualWithDrift(result, {'id':id, 'payload':'ThisIsThePayload', 'modified':float(ts)})

	def skiptestAdd_UnusualParentIDCharacters(self):
		"testAdd_UnusualParentIDCharacters: All bytes values from 00 to 255 are legal in a parent ID"
		userID, storageServer = self.createCaseUser()
		for i in range(0,256):
			id = '123\\00' + chr(i).encode("hex")
			ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'parentid':id, 'payload':'ThisIsThePayload'})

	def skiptestAdd_UnusualPredecessorIDCharacters(self):
		"testAdd_UnusualPredecessorIDCharacters: All bytes values from 00 to 255 are legal in a predecessor ID"
		userID, storageServer = self.createCaseUser()
		for i in range(0,256):
			id = '123\\00' + chr(i).encode("hex")
			ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'predecessorid': id, 'payload':'ThisIsThePayload'})
	
	def testAdd_IDTooBig(self):
		"testAdd_IDTooBig: An ID longer than 64 bytes should cause an error"
		userID, storageServer = self.createCaseUser()
		try:
			ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234567890123456789012345678901234567890123456789012345678901234567890', 'payload':'ThisIsThePayload'})
			self.fail("Should have reported error with too-big ID")
		except weave.WeaveException, e:
			self.failUnless(str(e).find("HTTP Error 400: Bad Request") > 0, "Should have been an HTTP 400 error")

	def testAdd_ParentIDTooBig(self):
		"testAdd_ParentIDTooBig: A parentID longer than 64 bytes should cause an error"
		userID, storageServer = self.createCaseUser()
		try:
			ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'parentid':'1234567890123456789012345678901234567890123456789012345678901234567890', 'payload':'ThisIsThePayload'})
			self.fail("Should have reported error with too-big parentID")
		except weave.WeaveException, e:
			self.failUnless(str(e).find("HTTP Error 400: Bad Request") > 0, "Should have been an HTTP 400 error")

	def testAdd_PredecessorIDTooBig(self):
		"testAdd_PredecessorIDTooBig: A predecessorID longer than 64 bytes should cause an error"
		userID, storageServer = self.createCaseUser()
		try:
			ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'predecessorid':'1234567890123456789012345678901234567890123456789012345678901234567890', 'payload':'ThisIsThePayload'})
			self.fail("Should have reported error with too-big predecessorID")
		except weave.WeaveException, e:
			self.failUnless(str(e).find("HTTP Error 400: Bad Request") > 0, "Should have been an HTTP 400 error")

	def testAdd_NonNumericSortIndex(self):
		"testAdd_NonNumericSortIndex: A non-numeric sortindex should cause an error"
		userID, storageServer = self.createCaseUser()
		try:
			ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'sortindex':'abc', 'payload':'ThisIsThePayload'})
			result = weave.get_item(storageServer, userID, self.password, 'coll', '1234')
			self.fail("Should have reported error with non-numeric SortIndex: got back %s" % result)
		except weave.WeaveException, e:
			self.failUnless(str(e).find("HTTP Error 400: Bad Request") > 0, "Should have been an HTTP 400 error")

	def testAdd_TooBigSortIndex(self):
		"testAdd_TooBigSortIndex: A sortindex longer than 11 bytes should cause an error"
		userID, storageServer = self.createCaseUser()
		try:
			ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'sortindex':'1234567890123', 'payload':'ThisIsThePayload'})
			result = weave.get_item(storageServer, userID, self.password, 'coll', '1234')
			self.fail("Should have reported error with too-big SortIndex: got back %s" % result)
		except weave.WeaveException, e:
			self.failUnless(str(e).find("HTTP Error 400: Bad Request") > 0, "Should have been an HTTP 400 error")

	def testAdd_NegativeSortIndex(self):
		"testAdd_NegativeSortIndex: A negative sortindex is fine."
		userID, storageServer = self.createCaseUser()
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'sortindex':'-5', 'payload':'ThisIsThePayload'})
		result = weave.get_item(storageServer, userID, self.password, 'coll', '1234')
		self.failUnlessObjsEqualWithDrift(result, {'id':'1234', 'payload':'ThisIsThePayload', 'modified':float(ts), 'sortindex':-5})

	def testAdd_FloatingPointSortIndex(self):
		"testAdd_FloatingPointSortIndex: A floating point sortindex will be rounded off."
		userID, storageServer = self.createCaseUser()
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'sortindex':'5.5', 'payload':'ThisIsThePayload'})
		result = weave.get_item(storageServer, userID, self.password, 'coll', '1234')
		self.failUnlessObjsEqualWithDrift(result, {'id':'1234', 'payload':'ThisIsThePayload', 'modified':float(ts), 'sortindex':5})
			
	def testAdd_ClientCannotSetModified(self):
		"testAdd_ClientCannotSetModified: An attempt by the client to set the modified field is ignored."
		userID, storageServer = self.createCaseUser()
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'modified':'123456789', 'payload':'ThisIsThePayload'})
		# server should impose its own modified stamp
		result = weave.get_item(storageServer, userID, self.password, 'coll', '1234')
		self.failUnlessAlmostEqual(float(ts), float(result['modified']))

	def testAdd_MissingPayload(self):
		"testAdd_MissingPayload: An attempt to put a new item without a payload should report an error."
		userID, storageServer = self.createCaseUser()
		try:
			ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'doesNotExist', 'parentid':'1234'})
			try:
				result = weave.get_item(storageServer, userID, self.password, 'coll', '1234')
				self.fail("Should have had an error on attempt to modify metadata of non-existent object: got %s" % str(result))
			except weave.WeaveException, e:
				self.fail("Should have had an error on attempt to modify metadata of non-existent object: the object was not created, but no error resulted")
		except weave.WeaveException, e:
			self.failUnless(str(e).find("HTTP Error 400: Bad Request") > 0, "Should have been an HTTP 400 error")

	def testAdd_MalformedJSON(self):
		"testAdd_MalformedJSON: An attempt to put an item with malformed JSON should report an error."
		userID, storageServer = self.createCaseUser()
		try:
			ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', """{'id':'abcd1234', 'payload':'ThisIsThePayload}""")
			self.fail("Should have had an error on attempt to modify metadata of non-existent object")
		except weave.WeaveException, e:
			self.failUnless(str(e).find("HTTP Error 400: Bad Request") > 0, "Should have been an HTTP 400 error")

	def testModify(self):
		"testModify: An object can be modified by putting to the collection"
		userID, storageServer = self.createCaseUser()
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'payload':'aPayload'})
		ts2 = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'payload':'aDifferentPayload'})
		result = weave.get_item(storageServer, userID, self.password, 'coll', '1234')
		self.failUnlessObjsEqualWithDrift(result, {'id':'1234', 'payload':'aDifferentPayload', 'modified':float(ts2)})

	def testModify_IDFromURL(self):
		"testModify_IDFromURL: An object can be modified by directly accessing its URL"
		userID, storageServer = self.createCaseUser()
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'sortindex':3, 'payload':'aPayload'})
		ts2 = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'sortindex':2, 'payload':'aDifferentPayload'}, urlID='1234')
		result = weave.get_item(storageServer, userID, self.password, 'coll', '1234')
		self.failUnlessObjsEqualWithDrift(result, {'id':'1234', 'payload':'aDifferentPayload', 'modified':float(ts2), 'sortindex':2})

	def testModify_sortIndex(self):
		"testModify_sortIndex: An object's sortIndex can be changed and does NOT update the modified date"
		userID, storageServer = self.createCaseUser()
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'sortindex':3, 'payload':'aPayload'})
		ts2 = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'sortindex':2})
		result = weave.get_item(storageServer, userID, self.password, 'coll', '1234')
		self.failUnlessObjsEqualWithDrift(result, {'id':'1234', 'payload':'aPayload', 'modified':float(ts), 'sortindex':2})

	def testModify_parentID(self):
		"testModify_parentID: An object's parentID can be changed"
		userID, storageServer = self.createCaseUser()
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'parentid':3, 'payload':'aPayload'})
		ts2 = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'parentid':2})
		result = weave.get_item(storageServer, userID, self.password, 'coll', '1234')
		self.failUnlessObjsEqualWithDrift(result, {'id':'1234', 'payload':'aPayload', 'modified':float(ts2), 'parentid':'2'})

	def testModify_predecessorID(self):
		"testModify_predecessorID: An object's predecessorID can be changed, and does NOT update the modified date"
		userID, storageServer = self.createCaseUser()
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'predecessorid':3, 'payload':'aPayload'})
		ts2 = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'predecessorid':2})
		result = weave.get_item(storageServer, userID, self.password, 'coll', '1234')
		self.failUnlessObjsEqualWithDrift(result, {'id':'1234', 'payload':'aPayload', 'modified':float(ts), 'predecessorid':'2'})
		# TODO: Changing the parentid changes the modification date, but changing the predecessorID does not.  Why?
	
	def testModify_ifModified_Modified(self):
		"testModify_ifModified_Modified: If an IfUnmodifiedSince header is provided, and the collection has changed, a modification attempt fails."
		userID, storageServer = self.createCaseUser()
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'sortindex':3, 'payload':'aPayload'})
		ts2 = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'payload':'newPayload'})
		try:
			ts3 = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'sortindex':1}, ifUnmodifiedSince=float(ts))
			result = weave.get_item(storageServer, userID, self.password, 'coll', '1234')
			self.fail("Attempt to modify an item when the collection had changed after the ifUnmodifiedSince time should have failed: got %s" % str(result))
		except weave.WeaveException, e:
			self.failUnless(str(e).find("HTTP Error 412: Precondition Failed") > 0, "Should have been an HTTP 412 error")

	def testAddMultiple(self):
		"testAddMultiple: A multiple add with some successes and some failures returns expected output."
		userID, storageServer = self.createCaseUser()
		objects = [
			{'id':'1', 'payload':'ThisIsThePayload'}, # all good
			{'id':'2'}, # missing payload
			{'id':'3', 'parentid':'a', 'predecessorid':'b', 'sortindex':3, 'payload':'A'}, # all good
			{'id':'4', 'parentid':''.join(['A' for i in range(70)]), 'payload':'A'}, # parent ID too long
			{'id':'5', 'predecessorid':''.join(['A' for i in range(70)]), 'payload':'A'}, # predecessor ID too long
			{'id':'6', 'sortindex':'blah', 'payload':'A'}, # sort index not integer
			{'payload':'payload'}, # missing ID
			{'id':'modifyme', 'sortindex':5, 'parentid':'a', 'predecessorid': 'b' }, # this will do a modify that changes the mod date
			{'id':'modifyme2', 'sortindex':5 }] # this will do a modify that does not change the mod date

		# Create the modify targets first
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'modifyme', 'payload':'ThisIsThePayload'})
		ts2 = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'modifyme2', 'payload':'ThisIsThePayload'})
			
		multiresult = weave.add_or_modify_items(storageServer, userID, self.password, 'coll', objects)

		# can't really check the value of modified, but it's supposed to be there
		self.failUnless("modified" in multiresult, "Result from a multi-object POST should contain a modified field.")
		self.failUnlessEqual(
			["1", "3", "modifyme", "modifyme2"], multiresult["success"])
		# TODO '2' fails silently right now; this is covered by a single test elsewhere
		self.failUnlessEqual(
			{'': ["invalid id"], "4": ['invalid parentid'], "5": ['invalid predecessorid'], "6": ['invalid sortindex']}, multiresult["failed"])

	def testAddMultiple_IfUnmodifiedSince_NotModified(self):
		"testAddMultiple_IfUnmodifiedSince_NotModified: If an IfUnmodifiedSince header is provided, and the collection has not changed, an attempt succeeds."
		userID, storageServer = self.createCaseUser()
		result = weave.add_or_modify_items(storageServer, userID, self.password, 'coll', [{'id':'1234', 'payload':'ThisIsThePayload'}])
		ts = weave.get_item(storageServer, userID, self.password, 'coll', '1234')['modified'] # TODO should use header
		weave.add_or_modify_items(storageServer, userID, self.password, 'coll', [{'id':'1234', 'payload':'ThisIsThePayload2'}], ifUnmodifiedSince=float(ts)+.01)
		result = weave.get_item(storageServer, userID, self.password, 'coll', '1234')
		self.failUnlessEqual(result['payload'], 'ThisIsThePayload2')

	def testAddMultiple_IfUnmodifiedSince_Modified(self):
		"testAddMultiple_IfUnmodifiedSince_Modified: If an IfUnmodifiedSince header is provided, and the collection has changed, the attempt fails."
		userID, storageServer = self.createCaseUser()
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'payload':'ThisIsThePayload'})
		ts2 = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'payload':'ThisIsThePayload2'})
		try:
			ts3 = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'payload':'ThisIsThePayload'}, ifUnmodifiedSince=float(ts)+.01)
			self.fail("Attempt to add an item when the collection had changed after the ifUnmodifiedSince time should have failed")
		except weave.WeaveException, e:
			self.failUnless(str(e).find("HTTP Error 412: Precondition Failed") > 0, "Should have been an HTTP 412 error")


	def testQuota(self):
		"testQuota: Storing an item should increase the quota usage for the user"
		userID, storageServer = self.createCaseUser()
		q = weave.get_quota(storageServer, userID, self.password)
		self.failUnlessEqual([0,None], q)
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'sortindex':3, 'payload':'aPayload'})
		q = weave.get_quota(storageServer, userID, self.password)
		self.failUnlessEqual([7,None], q, "If quotas are working, the quota should have been changed by an add call")
		# And we also need to test add (and modify) multiple

	def testCollection_SameIDs(self):
		"testCollection_SameIDs: Two objects with the same IDs can exist in different collections."
		userID, storageServer = self.createCaseUser()
		weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'payload':'aPayload'})
		weave.add_or_modify_item(storageServer, userID, self.password, 'coll2', {'id':'1234', 'payload':'aPayload'})
		weave.get_item(storageServer, userID, self.password, 'coll', '1234')
		weave.get_item(storageServer, userID, self.password, 'coll2', '1234')

	def testCollectionCounts(self):
		"testCollectionCounts: The count of objects should be updated correctly."
		userID, storageServer = self.createCaseUser()
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1', 'payload':'aPayload'})
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll2', {'id':'1', 'payload':'aPayload'})
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll3', {'id':'1', 'payload':'aPayload'})
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll4', {'id':'1', 'payload':'aPayload'})
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll3', {'id':'1', 'payload':'aNewPayload'})
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll4', {'id':'2', 'payload':'aPayload'})
		counts = weave.get_collection_counts(storageServer, userID, self.password)
		self.failUnlessEqual(counts, {"coll":"1", "coll2":"1","coll3":"1","coll4":"2"})

	def testCollectionTimestamps(self):
		"testCollectionTimestamps: The timestamps of objects should be returned correctly."
		userID, storageServer = self.createCaseUser()
		ts = {}
		ts['coll'] = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1', 'payload':'aPayload'})
		ts['coll2'] = weave.add_or_modify_item(storageServer, userID, self.password, 'coll2', {'id':'1', 'payload':'aPayload'})
		ts['coll3'] = weave.add_or_modify_item(storageServer, userID, self.password, 'coll3', {'id':'1', 'payload':'aPayload'})
		ts['coll4'] = weave.add_or_modify_item(storageServer, userID, self.password, 'coll4', {'id':'1', 'payload':'aPayload'})
		ts['coll3'] = weave.add_or_modify_item(storageServer, userID, self.password, 'coll3', {'id':'1', 'payload':'aNewPayload'})
		ts['coll4'] = weave.add_or_modify_item(storageServer, userID, self.password, 'coll4', {'id':'2', 'payload':'aPayload'})
		result = weave.get_collection_timestamps(storageServer, userID, self.password)
		for i in result.keys():
			self.failUnlessAlmostEqual(float(ts[i]), float(result[i]))

	def testCollectionIDs(self):
		"testCollectionIDs: The IDs should be returned correctly."
		userID, storageServer = self.createCaseUser()
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1', 'payload':'aPayload'})
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'2', 'payload':'aPayload'})
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'3', 'payload':'aPayload'})
		counts = weave.get_collection_ids(storageServer, userID, self.password, 'coll')
		self.failUnlessEqual(counts, ["1","2","3"])


	def testGet_multiple(self):
		"testGet_multiple: Attempt to get multiple objects with 'full'"
		userID, storageServer = self.createCaseUser()
		result = weave.add_or_modify_items(storageServer, userID, self.password, 'coll', [{'id':"id%s" % i, 'payload':'aPayload', 'sortindex': i} for i in range(1,3)])
		# TODO use the timestamp in the header for the assertion
		result = weave.get_collection_ids(storageServer, userID, self.password, 'coll', params="full=1")
		self.failUnlessEqual(result, 
			[{'id':"id%s" % i, 'payload':'aPayload', 'sortindex': i, 'modified': float(result[0]['modified'])} for i in range(1,3)])

	def testGet_NoObject(self):
		"testGet_NoObject: Attempt to get a non-existent object should return 404."
		userID, storageServer = self.createCaseUser()
		try:
			ts = weave.get_item(storageServer, userID, self.password, 'coll', 'noSuchObject')
			self.fail("Should have failed to get a non-existen object")
		except weave.WeaveException, e:
			self.failUnless(str(e).find("HTTP Error 404: Not Found") > 0, "Should have been an HTTP 404 error, was %s" % str(e))

	def testGet_NoAuth(self):
		"testGet_NoAuth: Attempt to get an object with no authorization should return a 401"
		userID, storageServer = self.createCaseUser()
		try:
			ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'abcd1234', 'payload':'aPayload'})
			ts = weave.get_item(storageServer, userID, None, 'coll', 'abcd1234', withAuth=False)
			self.fail("Should have raised an error for no authorization")
		except weave.WeaveException, e:
			self.failUnless(str(e).find("HTTP Error 401: Unauthorized") > 0, "Should have been an HTTP 401 error, was %s" % str(e))

	def testGet_BadPassword(self):
		"testGet_BadPassword: Attempt to get an object with wrong password should return an error"
		userID, storageServer = self.createCaseUser()
		try:
			ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'abcd1234', 'payload':'aPayload'})
			ts = weave.get_item(storageServer, userID, "wrongPassword", 'coll', 'abcd1234')
			self.fail("Should have raised an error for bad password")
		except weave.WeaveException, e:
			self.failUnless(str(e).find("HTTP Error 401: Unauthorized") > 0, "Should have been an HTTP 401 error, was %s" % str(e))

	def testGet_UserPathMismatch(self):
		"testGet_UserPathMismatch: Attempt to get an object with wrong user account should return an error"
		userID, storageServer = self.createCaseUser()
		userID2, storageServer2 = self.createCaseUser()
		try:
			ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'abcd1234', 'payload':'aPayload'})
			ts = weave.get_item(storageServer, userID, self.password, 'coll', 'abcd1234', withAuthUser=userID2)
			self.fail("Should have raised an error for cross-user access")
		except weave.WeaveException, e:
			# WEAVE_ERROR_USERID_PATH_MISMATCH
			self.failUnless(str(e).find("HTTP Error 400: Bad Request") > 0, "Should have been an HTTP 400 error, was %s" % str(e))

	def helper_testGet(self):
		'Helper function to set up many of the testGet functions'
		userID, storageServer = self.createCaseUser()
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1', 'payload':'aPayload', 'parentid':'ABC', 'predecessorid': 'abc', 'sortindex': '3'})
		ts2 = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'2', 'payload':'aPayload', 'parentid':'def', 'predecessorid': 'def', 'sortindex': '5'})
		ts3 = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'3', 'payload':'aPayload', 'parentid':'ABC', 'predecessorid': 'abc', 'sortindex': '1'})
		return (userID, storageServer, [ts, ts2, ts3])

	def testGet_ByParentID(self):
		"testGet_ByParentID: Attempt to get objects with a ParentID filter works"
		userID, storageServer, ts = self.helper_testGet()
		result = weave.get_collection_ids(storageServer, userID, self.password, 'coll', params="parentid=ABC")
		self.failUnlessEqual(['1', '3'], result)

	def testGet_ByPredecessorID(self):
		"testGet_ByPredecessorID: Attempt to get objects with a PredecessorID filter works"
		userID, storageServer, ts = self.helper_testGet()
		result = weave.get_collection_ids(storageServer, userID, self.password, 'coll', params="predecessorid=abc")
		self.failUnlessEqual(['1', '3'], result)

	def testGet_ByNewer(self):
		"testGet_ByNewer: Attempt to get objects with a Newer filter works"
		userID, storageServer, ts = self.helper_testGet()
		result = weave.get_collection_ids(storageServer, userID, self.password, 'coll', params="newer=%s" % ts[0])
		self.failUnlessEqual(['2', '3'], result)

	def testGet_ByOlder(self):
		"testGet_ByOlder: Attempt to get objects with a Older filter works"
		userID, storageServer, ts = self.helper_testGet()
		result = weave.get_collection_ids(storageServer, userID, self.password, 'coll', params="older=%s" % ts[2])
		self.failUnlessEqual(['1', '2'], result)

	def testGet_Sort_Oldest(self):
		"testGet_Sort_Oldest: Attempt to get objects with a sort 'oldest' parameter works"
		userID, storageServer, ts = self.helper_testGet()
		result = weave.get_collection_ids(storageServer, userID, self.password, 'coll', params="sort=oldest")
		self.failUnlessEqual(['1', '2', '3'], result)

	def testGet_Sort_Newest(self):
		"testGet_Sort_Newest: Attempt to get objects with a sort 'newest' parameter works"
		userID, storageServer, ts = self.helper_testGet()
		result = weave.get_collection_ids(storageServer, userID, self.password, 'coll', params="sort=newest")
		self.failUnlessEqual(['3', '2', '1'], result)

	def testGet_Sort_Index(self):
		"testGet_Sort_Index: Attempt to get objects with a sort 'index' parameter works"
		userID, storageServer, ts = self.helper_testGet()
		result = weave.get_collection_ids(storageServer, userID, self.password, 'coll', params="sort=index")
		self.failUnlessEqual(['2', '1', '3'], result)

	def testGet_Limit(self):
		"testGet_Limit: Attempt to get objects with a 'limit' parameter works"
		userID, storageServer, ts = self.helper_testGet()
		result = weave.get_collection_ids(storageServer, userID, self.password, 'coll', params="sort=index&limit=2")
		self.failUnlessEqual(['2', '1'], result)

	def testGet_Limit_Negative(self):
		"testGet_Limit_Negative: Attempt to get objects with a negative 'limit' should cause an error"
		userID, storageServer = self.createCaseUser()
		weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1', 'payload':'aPayload', 'sortindex': 5})
		try:
			result = weave.get_collection_ids(storageServer, userID, self.password, 'coll', params="sort=index&limit=-5")
			self.fail("Attempt to use offset without a negative limit should have raised an error; got %s" % result)
		except weave.WeaveException, e:
			self.failUnless(str(e).find("HTTP Error 400: Bad Request") > 0, "Should have been an HTTP 400 error, was %s" % str(e))

	def testGet_Offset(self):
		"testGet_Offset: Attempt to get objects with an 'offset' parameter works"
		userID, storageServer = self.createCaseUser()
		[weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':i, 'payload':'aPayload', 'sortindex': i}) for i in range(1,5)]
		result = weave.get_collection_ids(storageServer, userID, self.password, 'coll', params="sort=index&limit=2&offset=2")
		self.failUnlessEqual(['2', '1'], result) # should be 4,3,2,1; skip 2, limit 2

	def testGet_Offset_OffRange(self):
		"testGet_Offset: Attempt to get objects with an 'offset' higher than the highest value should return an empty set"
		userID, storageServer = self.createCaseUser()
		[weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':i, 'payload':'aPayload', 'sortindex': i}) for i in range(1,5)]
		result = weave.get_collection_ids(storageServer, userID, self.password, 'coll', params="sort=index&limit=2&offset=5")
		self.failUnlessEqual([], result) 

	def testGet_Offset_Negative(self):
		"testGet_Offset_Negative: Attempt to get objects with a negative 'offset' should cause an error"
		userID, storageServer = self.createCaseUser()
		weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1', 'payload':'aPayload', 'sortindex': 5})
		try:
			result = weave.get_collection_ids(storageServer, userID, self.password, 'coll', params="sort=index&limit=2&offset=-5")
			self.fail("Attempt to use offset with a negative offset should have raised an error; got %s" % result)
		except weave.WeaveException, e:
			self.failUnless(str(e).find("HTTP Error 400: Bad Request") > 0, "Should have been an HTTP 400 error, was %s" % str(e))

	def testGet_Offset_NoLimit(self):
		"testGet_Offset_NoLimit: Attempt to get objects with an 'offset' parameter without a 'limit' parameter should report an error"
		userID, storageServer = self.createCaseUser()
		[weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':i, 'payload':'aPayload', 'sortindex': i}) for i in range(1,5)]
		try:
			result = weave.get_collection_ids(storageServer, userID, self.password, 'coll', params="sort=index&offset=2")
			self.fail("Attempt to use offset without a limit should have raised an error; got %s" % result)
		except weave.WeaveException, e:
			self.failUnless(str(e).find("HTTP Error 400: Bad Request") > 0, "Should have been an HTTP 400 error, was %s" % str(e))


	def testGet_whoisi(self):
		"testGet_whoisi: Attempt to get multiple objects, specifying whoisi output format, without 'full'."
		userID, storageServer = self.createCaseUser()
		[weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':"id%s" % i, 'payload':'aPayload', 'sortindex': i}) for i in range(1,3)]
		result = weave.get_collection_ids(storageServer, userID, self.password, 'coll', asJSON=False, outputFormat="application/whoisi")
		self.failUnlessEqual("\x00\x00\x00\x05\"id1\"\x00\x00\x00\x05\"id2\"", result)

	def testGet_whoisi_full(self):
		"testGet_whoisi: Attempt to get multiple objects, specifying whoisi output format, with 'full'"
		userID, storageServer = self.createCaseUser()
		ts = [weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':"id%s" % i, 'payload':'aPayload', 'sortindex': i}) for i in range(1,3)]
		result = weave.get_collection_ids(storageServer, userID, self.password, 'coll', asJSON=False, params="full=1", outputFormat="application/whoisi")
		self.failUnlessEqual('\x00\x00\x00H{"id":"id1","modified":'+ts[0]+',"sortindex":1,"payload":"aPayload"}\x00\x00\x00H{"id":"id2","modified":'+ts[1]+',"sortindex":2,"payload":"aPayload"}',
			result)

	def testGet_newLines(self):
		"testGet_newLines: Attempt to get multiple objects, specifying newlines output format, without 'full'"
		userID, storageServer = self.createCaseUser()
		ts = [weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':"id%s" % i, 'payload':'aPayload', 'sortindex': i}) for i in range(1,3)]
		result = weave.get_collection_ids(storageServer, userID, self.password, 'coll', asJSON=False, params="full=1", outputFormat="application/newlines")
		self.failUnlessEqual(result,
			'{"id":"id1","modified":' + ts[0] + ',"sortindex":1,"payload":"aPayload"}\n{"id":"id2","modified":'+ ts[1] + ',"sortindex":2,"payload":"aPayload"}\n')


	def helper_testDelete(self):
		'Helper function to set up many of the testDelete functions'
		userID, storageServer = self.createCaseUser()
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1', 'payload':'aPayload', 'parentid':'ABC', 'predecessorid': 'abc', 'sortindex': '3'})
		ts2 = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'2', 'payload':'aPayload', 'parentid':'def', 'predecessorid': 'def', 'sortindex': '5'})
		ts3 = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'3', 'payload':'aPayload', 'parentid':'ABC', 'predecessorid': 'abc', 'sortindex': '1'})
		return (userID, storageServer, [ts, ts2, ts3])

	def testDelete(self):
		"testDelete: Attempt to delete objects by ID should work"
		userID, storageServer, ts = self.helper_testDelete()
		ts = weave.delete_item(storageServer, userID, self.password, 'coll', '1')
		result = weave.get_collection_ids(storageServer, userID, self.password, 'coll')
		self.failUnlessEqual(['2', '3'], result)
		try:
			ts2 = weave.get_item(storageServer, userID, self.password, 'coll', '1')
			self.fail("Should have raised a 404 exception on attempt to access deleted object")
		except weave.WeaveException, e:
			self.failUnless(str(e).find("HTTP Error 404: Not Found") > 0, "Should have been an HTTP 404 error")

	def testDelete_ByParentID(self):
		"testDelete_ByParentID: Attempt to delete objects with a ParentID filter works"
		userID, storageServer, ts = self.helper_testDelete()
		ts = weave.delete_items(storageServer, userID, self.password, 'coll', params="parentid=ABC")
		result = weave.get_collection_ids(storageServer, userID, self.password, 'coll')
		self.failUnlessEqual(['2'], result)

	def testDelete_ByPredecessorID(self):
		"testDelete_ByPredecessorID: Attempt to delete objects with a PredecessorID filter works"
		userID, storageServer, ts = self.helper_testDelete()
		result = weave.delete_items(storageServer, userID, self.password, 'coll', params="predecessorid=abc")
		result = weave.get_collection_ids(storageServer, userID, self.password, 'coll')
		self.failUnlessEqual(['2'], result)

	def testDelete_ByNewer(self):
		"testDelete_ByNewer: Attempt to delete objects with a Newer filter works"
		userID, storageServer, ts = self.helper_testDelete()
		result = weave.delete_items(storageServer, userID, self.password, 'coll', params="newer=%s" % ts[0])
		result = weave.get_collection_ids(storageServer, userID, self.password, 'coll')
		self.failUnlessEqual(['1'], result)

	def testDelete_ByOlder(self):
		"testDelete_ByOlder: Attempt to delete objects with a Older filter works"
		userID, storageServer, ts = self.helper_testDelete()
		result = weave.delete_items(storageServer, userID, self.password, 'coll', params="older=%s" % ts[2])
		result = weave.get_collection_ids(storageServer, userID, self.password, 'coll')
		self.failUnlessEqual(['3'], result)

	def testDelete_Sort_Oldest(self):
		"testDelete_Sort_Oldest: Attempt to delete objects with a sort 'oldest' parameter works"
		userID, storageServer, ts = self.helper_testDelete()
		result = weave.delete_items(storageServer, userID, self.password, 'coll', params="sort=oldest&limit=2")
		result = weave.get_collection_ids(storageServer, userID, self.password, 'coll')
		self.failUnlessEqual(['3'], result)

	def testDelete_Sort_Newest(self):
		"testDelete_Sort_Newest: Attempt to delete objects with a sort 'newest' parameter works"
		userID, storageServer, ts = self.helper_testDelete()
		result = weave.delete_items(storageServer, userID, self.password, 'coll', params="sort=newest&limit=2")
		result = weave.get_collection_ids(storageServer, userID, self.password, 'coll')
		self.failUnlessEqual(['1'], result)

	def testDelete_Sort_Index(self):
		"testDelete_Sort_Index: Attempt to delete objects with a sort 'index' parameter works"
		userID, storageServer, ts = self.helper_testDelete()
		result = weave.delete_items(storageServer, userID, self.password, 'coll', params="sort=index&limit=2")
		result = weave.get_collection_ids(storageServer, userID, self.password, 'coll')
		self.failUnlessEqual(['3'], result)

	def testDelete_Limit(self):
		"testDelete_Limit: Attempt to delete objects with a 'limit' parameter works"
		userID, storageServer, ts = self.helper_testDelete()
		result = weave.delete_items(storageServer, userID, self.password, 'coll', params="sort=index&limit=1&offset=1")
		result = weave.get_collection_ids(storageServer, userID, self.password, 'coll')
		self.failUnlessEqual(['1', '3'], result)
		
	def testDelete_indexAbove(self):
		"testDelete_indexAbove: Attempt to delete objects with an 'index_above' parameter works"
		userID, storageServer, ts = self.helper_testDelete()
		result = weave.delete_items(storageServer, userID, self.password, 'coll', params="index_above=2")
		result = weave.get_collection_ids(storageServer, userID, self.password, 'coll')
		self.failUnlessEqual(['3'], result, "Items with an index above 2 should have been deleted")
		# BUG wrong variable in params

	def testDelete_indexBelow(self):
		"testDelete_indexBelow: Attempt to delete objects with an 'index_below' parameter works"
		userID, storageServer, ts = self.helper_testDelete()
		result = weave.delete_items(storageServer, userID, self.password, 'coll', params="index_below=4")
		result = weave.get_collection_ids(storageServer, userID, self.password, 'coll')
		self.failUnlessEqual(['2'], result, "Items with an index below 4 should have been deleted")
		# BUG wrong variable in params

	def testDelete_IfUnmodifiedSince_NotModified(self):
		"testDelete_IfUnmodifiedSince_NotModified: If an IfUnmodifiedSince header is provided, and the collection has not changed, the attempt succeeds."
		userID, storageServer = self.createCaseUser()
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'payload':'ThisIsThePayload'})
		ts2 = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'payload':'ThisIsThePayload2'})
		result = weave.delete_item(storageServer, userID, self.password, 'coll', '1234', ifUnmodifiedSince=ts2)
		try:
			weave.get_item(storageServer, userID, self.password, 'coll', '1234')
			self.fail("Should have raised a 404 exception on attempt to access deleted object")
		except weave.WeaveException, e:
			self.failUnless(str(e).find("HTTP Error 404: Not Found") > 0, "Should have been an HTTP 404 error")



	def testDelete_IfUnmodifiedSince_Modified(self):
		"testDelete_IfUnmodifiedSince_Modified: If an IfUnmodifiedSince header is provided, and the collection has changed, the attempt fails."
		userID, storageServer = self.createCaseUser()
		ts = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'payload':'ThisIsThePayload'})
		ts2 = weave.add_or_modify_item(storageServer, userID, self.password, 'coll', {'id':'1234', 'payload':'ThisIsThePayload2'})
		try:
			result = weave.delete_item(storageServer, userID, self.password, 'coll', '1234', ifUnmodifiedSince=ts)
			self.fail("Attempt to delete an item that hasn't modified, with an ifModifiedSince header, should have failed")
		except weave.WeaveException, e:
			self.failUnless(str(e).find("HTTP Error 412: Precondition Failed") > 0, "Should have been an HTTP 412 error")




# TODO: Test X-Weave-Timestamp header

	


# Doc bugs:
# predecessorID is not documented in DELETE
# indexAbove and indexBelow are not documented in DELETE
# Behavior of offset when limit is missing is not defined

class TestStorageLarge(unittest.TestCase):

	def setUp(self):
		self.userID = 'weaveunittest_' + ''.join([chr(random.randint(ord('a'), ord('z'))) for i in xrange(10)])
		self.email = 'testuser@test.com'
		self.password = 'mypassword'
		self.failUnless(weave.checkNameAvailable(SERVER_BASE, self.userID))
		weave.createUser(SERVER_BASE, self.userID, self.password, self.email)
		self.failIf(weave.checkNameAvailable(SERVER_BASE, self.userID))
		self.storageServer = weave.getUserStorageNode(SERVER_BASE, self.userID, self.password)

	def testStorage(self):
		item1 = '{"id": 1, "sortindex": 1, "payload": "123456789abcdef"}'
		item2 = '{"id": 2, "sortindex": 2, "payload":"abcdef123456789"}'
		item3 = '{"id": 3, "parentid": 1, "sortindex": 3, "payload":"123abcdef123456789"}'
		item4 = '{"id": 4, "parentid": 1, "sortindex": 4, "payload":"567abcdef123456789"}'
		item5 = '{"parentid": 1, "sortindex": 4, "payload":"567abcdef123456789"}'
		item4_update = '{"id": 4, "parentid": 1, "sortindex": 5}' 

		TEST_WEAVE_QUOTA = True
		timestamp1 = weave.add_or_modify_item(self.storageServer, self.userID, self.password, 'history', item1)
		self.failUnless(abs(time.time() - float(timestamp1)) < 10, "Timestamp drift between client and server must be <10 sec") # no more than 10 seconds of drift
		
		#if TEST_WEAVE_QUOTA:
		#	quota = weave.get_quota(SERVER_BASE, self.userID, self.password)
		timestamp2 = weave.add_or_modify_item(self.storageServer, self.userID, self.password, 'foo', item2);

		counts = weave.get_collection_counts(self.storageServer, self.userID, self.password)
		self.failUnlessEqual({'history':'1', 'foo':'1'}, counts)

		timestamps = weave.get_collection_timestamps(self.storageServer, self.userID, self.password)
		self.failUnlessEqual({'history':float(timestamp1), 'foo':float(timestamp2)}, timestamps)
		
		result = weave.get_item(self.storageServer, self.userID, self.password, 'foo', '2')
		self.failUnlessEqual(result['id'], '2')
		self.failUnlessEqual(result['sortindex'], 2)
		self.failUnlessEqual(result['payload'], "abcdef123456789")
		self.failUnlessAlmostEqual(result['modified'], float(timestamp2)) # float drift
		
		result = weave.get_collection_ids(self.storageServer, self.userID, self.password, 'foo')
		self.failUnlessEqual(['2'], result)

		try:
			result = weave.add_or_modify_item(self.storageServer, self.userID, self.password, 'foo', item1, ifUnmodifiedSince=float(timestamp2)-1)
			self.fail("Should have failed on too-old timestamp")
		except weave.WeaveException, e:
			pass
		
		result = weave.add_or_modify_items(self.storageServer, self.userID, self.password, 'foo', 
			"[%s,%s,%s]" % (item3, item4, item5))
		self.failUnlessEqual({'failed':{'':['invalid id']},'success':['3', '4']}, result)
		
		result = weave.get_collection_ids(self.storageServer, self.userID, self.password, 'foo', "sort=index")
		self.failUnlessEqual(['4', '3', '2'], result)
		
		result = weave.get_collection_ids(self.storageServer, self.userID, self.password, 'foo', "sort=index&parentid=1")
		self.failUnlessEqual(['4', '3'], result)

		timestamp3 = weave.delete_item(self.storageServer, self.userID, self.password, 'foo', '3')
		result = weave.get_collection_ids(self.storageServer, self.userID, self.password, 'foo', "sort=index")
		self.failUnlessEqual(['4', '2'], result, "ID 3 should have been deleted")

		counts = weave.get_collection_counts(self.storageServer, self.userID, self.password)
		self.failUnlessEqual({'history':'1', 'foo':'2'}, counts)

		timestamp4 = weave.add_or_modify_item(self.storageServer, self.userID, self.password, 'foo', item4_update) # bump sortindex up
		result = weave.get_item(self.storageServer, self.userID, self.password, 'foo', '4')
		self.failUnlessAlmostEqual(result['modified'], float(timestamp4)) # float drift		
		del result['modified']
		self.failUnlessEqual({'id':'4', 'parentid':'1', 'sortindex': 5, 'payload':'567abcdef123456789'}, result)
	
		timestamp5 = weave.delete_items_older_than(self.storageServer, self.userID, self.password, 'foo', float(timestamp2) + .01)
		counts = weave.get_collection_counts(self.storageServer, self.userID, self.password)
		self.failUnlessEqual({'history':'1', 'foo':'1'}, counts)

		timestamps = weave.get_collection_timestamps(self.storageServer, self.userID, self.password)
		self.failUnlessEqual({'history':float(timestamp1), 'foo':float(timestamp4)}, timestamps)

		try:
			result = weave.delete_all(self.storageServer, self.userID, self.password, confirm=False)
			self.fail("Should have received an error for delete without confirmatation header")
		except weave.WeaveException, e:
			pass

		timestamp = weave.delete_all(self.storageServer, self.userID, self.password)


	def testBadMethod(self):
		req = urllib2.Request("%s/1.0/%s" % (self.storageServer, self.userID))
		req.get_method = lambda: 'HEAD'
		try:
			f = opener.open(req)
			result = f.read()
			self.fail("Should have reported an error on a HEAD attempt")
		except urllib2.URLError, e:
			pass
			
	def skiptestBadFunction(self):
		req = urllib2.Request("%s/1.0/%s/badfunction/" % (self.storageServer, self.userID))
		base64string = base64.encodestring('%s:%s' % (self.userID, self.password))[:-1]
		req.add_header("Authorization", "Basic %s" % base64string)
		f = opener.open(req)
		result = f.read()
		# this should be allowed and return an empty body
		self.failUnlessEqual("", result)
			
