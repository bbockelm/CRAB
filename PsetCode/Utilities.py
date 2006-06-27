#!/usr/bin/env python
"""
_Utilities_

General Tools to assist in PSet manipulation

"""


def isQuoted(theString):
    """
    _isQuoted_

    return True if a string is enclosed in single or double quotes
    False otherwise

    """
    if theString.startswith("\"") and theString.endswith("\""):
        return True
    if theString.startswith("\'") and theString.endswith("\'"):
        return True
    return False


def unQuote(theString):
    """
    _unQuote_

    Strip off leading and trailing escaped quotes

    """
    while theString.startswith("\"") or theString.startswith("\'"):
        theString = theString[1:]
    while theString.endswith("\"") or theString.endswith("\'"):
        theString = theString[:-1]
    return theString

