-- Databricks notebook source
SELECT SUM(sum_numeric_position) FROM flash_report_boxcode_aggregates 
WHERE 
(table_c = 'AL.02.02.02') AND 
(boxc = 'ALEbox7' OR boxc = 'ALCbox8') AND 
(ROW_map = 'R0300') AND 
(COL = 'C0020' OR COL = 'C0030') AND 
(Z = 'Z0010') AND 
(MET = 'boe_met:mi9044') AND 
(CUD = 'eba_CU:EUR (Euro)' OR CUD = 'boe_eba_CU:x9001 (Currencies other than Pound Sterling and Euro including gold and SDR)') AND 
(TYA = 'x0') AND 
(MCY = 'boe_eba_MC:x9244') AND 
(BAS = 'eba_BA:x17 (Assets)') AND 
(CPS = "boe_eba_CT:x9068 (Deposit-taking corporations [counterparty's home country definition])") AND 
(RCP = 'eba_GA:GB') AND 
(RPR = 'x0') AND 
(MCB = 'eba_MC:x9343');
