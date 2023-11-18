from arelle import ModelManager, Cntlr
import json


cntlr = Cntlr.Cntlr(logFileName="logToPrint.txt")
model_manager = ModelManager.initialize(cntlr)
model_xbrl = model_manager.load(r"data\current\ACGL/000094748423000015/acgl-20221231.htm")







# variable tablename tag topparent


# check for AssetsAbstract, in facts and factsByQname. to get local variable name. or should go table extraction.

# model_xbrl.factsByQname

for fact in model_xbrl.factsByQname:
    if str(fact) == 'us-gaap:AssetsAbstract':
        a = 1


# with open(r"")
# hierarchy = 

print("a")




