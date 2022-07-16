# !/usr/bin/python

############################################################################################
# file_name: posTransAgg.py
# Objective: Aggregates data at both customer and transaction level. 
#
# References:
# https://stackoverflow.com/questions/43232169/converting-a-dataframe-into-json-in-pyspark-and-then-selecting-desired-fields
#
############################################################################################

import logging
import traceback
import json

try:
    import common
except Exception as e:
    from main import common

class PosTransAggregation:

    def __init__(self,CommonFunctions,config):
        self.config = config
        self.CommonFunctions = CommonFunctions
    
    """
        Prints the output to STDOUT in JSON format
    """
    def print_json(data):
        results = data.toJSON().collect()
        for i in results:
            print(i)
    
    """
        Execution Layer to create dataframes and find aggregates for different customers and credit card
        transactions
    """
    def execute(self):
        try:
            for source in self.config.get("sources"):
                data = self.CommonFunctions.read_files(source.get("format"),source.get("file_path"))
                data.createOrReplaceTempView(source.get("view_name"))

            print("PosTransAggregation: Total credit card transaction value by month")
            trans_base = self.CommonFunctions.spark.sql("select months, \
                                                   sum(cast(Transaction_Value as Decimal(18,0))) as Transaction_Value  \
                                                   from( \
                                                   select substring(lpad(Transaction_Date,9,'0'),4,3) as months, \
                                                   Transaction_Value as Transaction_Value \
                                                   from vw_TransactionBase \
                                                   ) a \
                                                   group by months")
            PosTransAggregation.print_json(trans_base) 

            print("PosTransAggregation: Total credit card transaction amount by Customer_Segment")
            trans_cust_seg = self.CommonFunctions.spark.sql("select c.Customer_Segment, \
                                                       sum(cast(a.Transaction_Value as Decimal(18,0))) as Transaction_Value \
                                                       from vw_TransactionBase a inner join vw_CardBase b \
                                                       on a.Credit_Card_ID = b.Card_Number \
                                                       inner join vw_CustomerBase c \
                                                       on b.Cust_ID = c.Cust_ID \
                                                       group by c.Customer_Segment")
            PosTransAggregation.print_json(trans_cust_seg)

            print("PosTransAggregation: Average age of customers by Customer_Segment")
            avg_age_seg = self.CommonFunctions.spark.sql("select Customer_Segment, \
                                                    cast(avg(age) as Decimal(3,0)) as Avg_Age from vw_CustomerBase \
                                                    group by Customer_Segment")
            PosTransAggregation.print_json(avg_age_seg)


            print("Minimum, Max and Average credit card transaction value by Card_Family")
            card_agg_trans = self.CommonFunctions.spark.sql("select b.Card_Family, \
                                                       min(cast(a.Transaction_Value as Decimal(18,0))) as Min_Transaction_Value, \
                                                       max(cast(a.Transaction_Value as Decimal(18,0))) as Max_Transaction_Value, \
                                                       avg(cast(a.Transaction_Value as Decimal(38,0))) as Avg_Transaction_Value \
                                                       from vw_TransactionBase a inner join vw_CardBase b \
                                                       on a.Credit_Card_ID = b.Card_Number \
                                                       inner join vw_CustomerBase c \
                                                       on b.Cust_ID = c.Cust_ID \
                                                       group by b.Card_Family")
            PosTransAggregation.print_json(card_agg_trans)

            print("Top 5 customers who have made the most credit card transaction spend")
            top_cust_spend = self.CommonFunctions.spark.sql("select d.Cust_ID, \
                                                       d.Transaction_Value \
                                                       from (select b.Cust_ID as Cust_ID, \
                                                       sum(cast(a.Transaction_Value as Decimal(18,0))) as Transaction_Value \
                                                       from vw_TransactionBase a inner join vw_CardBase b \
                                                       on a.Credit_Card_ID = b.Card_Number \
                                                       inner join vw_CustomerBase c \
                                                       on b.Cust_ID = c.Cust_ID \
                                                       group by b.Cust_ID) d \
                                                       order by d.Transaction_Value desc \
                                                       LIMIT 5")
            PosTransAggregation.print_json(top_cust_spend)
            return
        except Exception as e:
                    common.logger.error("PosTrans Failed")
                    common.logger.error(e)
                    sys.exit(1)