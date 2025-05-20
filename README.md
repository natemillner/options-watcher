# Options Watcher

This repo is a options classifier built on the tradier api


# Algoritms

I am currently using the Lee/Ready (1991) algo for classifying trades, this works in my favor as tradeir gives me individual trades already grouped with bid/ask.
Bulk Volume Classification (BVC) Would be another choice that I may implement in the future. 


# Database

Redis is used for short term storage like previous trade price / classification.
Postgres is used to store all trades and symbols to be futher analyzed elsewhere.

## Contact

For questions or feedback, please contact [natemillner@gmail.com](mailto:natemillner@gmail.com).