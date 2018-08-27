Frequent item analysis on the Instacart Market Basket dataset. 
Below are the steps of the project:
1. Load dataset using the path specified in the first argument to the class.
2. Create Transactions from the dataset. You only need two fields: \order id" and \product id". Each order id represents one transaction.
3. Create FPGrowth model.
4. Find Frequent Itemsets: Write the top 10 most frequent output to a file specified by the output argument.
5. Generate Association Rules: model.generateAssociationRules method to generate high-confidence association rules.