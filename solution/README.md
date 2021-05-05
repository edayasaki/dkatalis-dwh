# Solution

Steps:
- Retrieve all data (JSON) and then combine into single dataset for each of them: 'accounts', 'savings' and 'cards' repectively
- Update the 'data' parts information of each dataset from the 'set' parts to complete the historical view
- Add 'datetime' field from timestamp and sort all datasets by timestamp 
- Calculate the 'transaction' field for 'savings' transaction and 'cards' transaction
- Join and merge all datasets log-history of 'accounts', 'savings' and 'cards' by linking 'card_id' and 'saving_accounts_id', yielding 'history' dataset
- Filter out transaction information from 'history' dataset and create 'transaction' dataset
- Print out results by each and linked dataset and by transaction summary dataset to web page (HTML).
