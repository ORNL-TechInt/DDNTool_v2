#!/usr/bin/python
#
# Python code to perform 'bracket expansion' similar to Bash shell.
# Expects a list of strings containing bracket expressions such as:
# "name[1-3][a,b]"
# and expands them into a (longer) list of strings with the bracket
# expressions evaluated.  The above example would be expanded to:
# "name1a", "name2a", "name3a", "name1b", "name2b", "name3b",
# "name1c", "name2c", "name3c"
#
# There are 2 types of bracket expression allowed:
# itemized - where each token is separated by a comma
# incremental - which is used to specify a range of numbers
# 
# Incremental bracket expressions must consist of exactly 2
# integers separated by a dash and the second integer must
# be greater than the first.  That expression will be expanded
# out into a list of integers from the first token to the second
# (inclusive)
#
# Itemized bracket expressions may have any number of tokens, and
# those tokens may be strings of arbitrary length and containing
# any character except the comma or dash.
#
# Bracket expressions may not be nested!
#
# No guarantees are made about the order of the results.
#
# Note: This file is its own unit-test.  (See the 
# "if __name__=='__main__'" test at the bottom.)


class BracketGrammarError(Exception):
    "Used to indicate an improper bracket expression of some kind"


def bracket_expand( thelist):
    item_num = 0
    while item_num < len(thelist):
    # Note: using 'while' instead of a more usual 'for... in...'
    # because we're actually going to be modifying thelist in
    # the body of this loop
        item = thelist[item_num]
        bracket_start = item.find( '[')
        if bracket_start != -1:
            bracket_end = item.find( ']', bracket_start)
            initial_part = item[0:bracket_start]
            final_part = item[bracket_end+1:]
            
            # figure out if it's an itemized or incremental expression
            if item.find(',', bracket_start, bracket_end) != -1:
                new_items = _expand( initial_part, final_part, item[bracket_start+1:bracket_end].split(','))
            else:
                values = item[bracket_start+1:bracket_end].split('-')
                if len(values) != 2:
                    raise BracketGrammarError, "Incremental expressions must consist of exactly 2 numbers separated by a dash"
                
                start = int(values[0])
                end = int(values[1])
                if not end > start:
                    raise BracketGrammarError, "End value of incremental expression must be greater than the start value"
                    
                new_items = _expand( initial_part, final_part, range( start, end+1))
                
            thelist.pop(item_num) # remove the original item from the front of the list
            thelist.extend( new_items) # add the new items to the end
        else:
            # didn't find an item with a bracket, so increment item_num
            item_num += 1;
            

def bracket_aware_split( thestring):
    '''
    Similar to the regular <string>.split(',') function, but is aware of
    bracket expressions and won't trigger on commas inside of them.
    '''
    out = []
    n = start = 0
    inside_bracket = False
    while n <  len(thestring):
        if thestring[n] == '[':
            inside_bracket = True
        elif thestring[n] == ']':
            inside_bracket = False
        elif thestring[n] ==',':
            if not inside_bracket:
                # Found a comma outside of a bracket expression. Append
                # everything up to the comma the out list
                out.append( thestring[start:n])
                start = n+1
        n += 1
    
    # Append the last part of thestring to out
    out.append(thestring[start:])

    return out

    

def _expand( initial_part, final_part, expr_list):
# Return a list of strings created by concatenating initial_part,
# one item of expr_list and final_part
# Items in expr_list must be 'stringifiable'
    out = []
    for e in expr_list:
        out.append( initial_part + str(e) + final_part)
        
    return out
    
# Unit Test for bracket expansion
if __name__=='__main__':
    test_string = "no expansion,increment expansion: [1-5],itemized expansion: [left,right,front,back],quark:[,anti-][top,bottom,strange,charm,up,down]"
    
    # test_string should expand to the following
    expected = ['no expansion',
                'increment expansion: 1', 'increment expansion: 2', 'increment expansion: 3', 'increment expansion: 4', 'increment expansion: 5',
                'itemized expansion: left', 'itemized expansion: right', 'itemized expansion: front', 'itemized expansion: back',
                'quark:top', 'quark:bottom', 'quark:strange', 'quark:charm', 'quark:up', 'quark:down',
                'quark:anti-top', 'quark:anti-bottom', 'quark:anti-strange', 'quark:anti-charm', 'quark:anti-up', 'quark:anti-down']
    
    
    test_list = bracket_aware_split( test_string)
    bracket_expand( test_list)
    
    if test_list == expected:
        print "Results as expected. Test passed."
    else:
        print "Result mismatch.  Test failed."
        print "Details:"
        print
        
        # check for the exact mis-match
        if len(test_list) != len(expected):
            print "Test list had %d items after expansion.  Expected it to have %d items."%(len(test_list), len(expected))
        
        item_num = 0;
        while item_num < min(len(test_list), len(expected)):
            if test_list[item_num] != expected[item_num]:
                print "First mismatch (element %d): '%s' != '%s'"%(item_num, test_list[item_num], expected[item_num])
                break
            item_num+=1
   
