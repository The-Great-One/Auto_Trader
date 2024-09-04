from RULE_SET_1 import Rule_1
from KITE_TRIGGER_ORDER import handle_decisions

def Apply_Rules(rt_data):
    decisions = list()
    decisions.extend(Rule_1(rt_data))
    
    if decisions: 
        handle_decisions(decisions=decisions)