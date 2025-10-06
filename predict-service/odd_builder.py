from bson.objectid import ObjectId
from services.db import groups, custom_odds

#group = groups.find_one({'name': 'Test group'})
operators = ['and', 'or', '=', '>', '>=', '<', '<=', '!=', '+', '-']
comparators = ['=', '>', '>=', '<', '<=', '!=', ]
combinators = ['and', 'or']
calculators = ['+', '-']


def switch_operator(val1, operator, val2):
    if operator == 'and':
        return val1 and val2
    elif operator == 'or':
        return val1 or val2
    elif operator == '=':
        return val1 == val2
    elif operator == '>':
        return val1 > val2
    elif operator == '>=':
        return val1 >= val2
    elif operator == '<':
        return val1 < val2
    elif operator == '<=':
        return val1 <= val2
    elif operator == '!=':
        return val1 != val2
    elif operator == '+':
        return val1 + val2
    elif operator == '-':
        return val1 - val2


def getOperand(line, statLine):
    if line['type'] == 'value':
        return line['value']
    elif line['type'] == 'stat':
        return statLine[line['preffix'] + ' ' + line['stat']]


def build_odd_lines(lines, statLine):

    #calculators
    value_buffer = getOperand(lines[0], statLine)
    low_priority_operators = []
    for i, line in enumerate(lines[1:]):
        if line['operator'] in calculators:
            value_buffer = switch_operator(
                value_buffer, line['operator'], getOperand(line, statLine))
        else:
            low_priority_operators.append(value_buffer)
            low_priority_operators.append(line['operator'])
            value_buffer = getOperand(line, statLine)
    low_priority_operators.append(value_buffer)

    #comparators
    value_buffer = low_priority_operators[0]
    lower_priority_operators = []
    i = 1
    while i < len(low_priority_operators):
        if low_priority_operators[i] in comparators:
            value_buffer = switch_operator(
                value_buffer, low_priority_operators[i], low_priority_operators[i+1])
        else:
            lower_priority_operators.append(value_buffer)
            lower_priority_operators.append(low_priority_operators[i])
            value_buffer = low_priority_operators[i+1]
        i += 2
    lower_priority_operators.append(value_buffer)
    
    #combinators
    value_buffer = lower_priority_operators[0]
    i = 1
    while i < len(lower_priority_operators):
        value_buffer = switch_operator(
            value_buffer, lower_priority_operators[1], lower_priority_operators[2])
        i += 2
    return 1 if value_buffer == True else 0


def build_odd_chain(odds, statLine):
    odd_results = {}
    for odd in odds:
        custom_odd = custom_odds.find_one({'_id': ObjectId(odd['_id'])})
        odd_results[custom_odd['name']] = build_odd_lines(
            custom_odd['lines'], statLine)
    return odd_results


def extract_odds_from_group(group, statLine):
    return build_odd_chain(group['items'], statLine)
