from Auto_Trader.RULE_SET_1 import Rule_1
from Auto_Trader.KITE_TRIGGER_ORDER import handle_decisions

def Apply_Rules(q):
    rt_data = q.get()
    decisions = list()
    decisions.extend(Rule_1(rt_data))
    
    if decisions: 
        handle_decisions(decisions=decisions)