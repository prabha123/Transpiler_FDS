-- Databricks notebook source
SELECT SUM(sum_numeric_position) FROM flash_report_boxcode_aggregates 
WHERE 
(table_c = 'DQ.01.01.01') AND 
(boxc = 'DQbox1' OR boxc = 'DQbox2' OR boxc = 'DQbox3' OR boxc = 'DQbox4' OR boxc = 'DQbox5' OR boxc = 'DQbox6') AND 
(ROW_map = 'R0250' OR ROW_map = 'R0260' OR ROW_map = 'R0270') AND 
(COL = 'C0030' OR COL = 'C0020') AND 
(Z = 'Z0010') AND 
(MET = 'boe_met:mi9001 (Market value [Statistics])') AND 
(CUD = 'boe_eba_CU:x9001 (Currencies other than Pound Sterling and Euro including gold and SDR)' OR CUD = 'eba_CU:EUR (Euro)') AND 
(TYA = 'x0') AND 
(MCY = 'eba_MC:x99 (Derivatives)' OR MCY = 'eba_MC:x9244') AND 
(BAS = 'eba_BA:x6 (Assets)') AND 
(CPS = "boe_eba_CT:x9157 (Deposit-taking corporations [counterparty's home country definition])" OR CPS = "boe_eba_CT:x9158 (Deposit-taking corporations [counterparty's home country definition])" OR CPS = "boe_eba_CT:x9158 (Central Monetary Institutions [counterparty's home country definition])") AND 
(RCP = 'boe_eba_GA:x9001 (Non UK resident)') AND 
(RPR = 'boe_eba_RP:x9023 (Entities of the group [Statistics])' OR RPR = 'x0' OR RPR = 'boe_eba_RP:x9006 (Other than entities of the group)') AND 
(MCB = 'x0');
