-- Databricks notebook source
SELECT SUM(sum_numeric_position) FROM flash_report_boxcode_aggregates 
WHERE 
(table_c = 'AL.02.01.01') AND 
(boxc = 'ALbox1' OR boxc = 'ALbox2') AND 
(ROW_map = 'R0030') AND 
(COL = 'C0030' OR COL = 'C0020') AND 
(Z = 'Z0010') AND 
(MET = 'boe_met:mi9004 (outstanding amount and nomiminal value)') AND 
(CUD = 'boe_eba_CU:x9001' OR CUD = 'eba_CU:EUR (Euro)') AND 
(TYA = 'x0') AND 
(MCY = 'eba_MC:x99 (Derivatives)') AND 
(BAS = 'eba_BA:x17 (Assets)') AND 
(CPS = "boe_eba_CT:x9157 (Deposit-taking corporations [counterparty's home country definition])") AND 
(RCP = 'eba_GA:GB') AND 
(RPR = 'boe_eba_RP:x9006 (Other than entities of the group)') AND 
(MCB = 'boe_eba_MC:x9344');
