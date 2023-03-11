# Project Laramie

This is a paper-trading hedge fund I'm maintaining with friends and colleagues. Each person is responsible for their own trades, and can do any sort of analysis they want (fundamental, quantitative, macroeconomic, technical and so on). The code in this repository contains the main scripts used to commit trades and calculate the performance, risk measures and current portfolios. 

As of now, the fund can trade equities, etfs, options and foreign exchange pairs from Yahoo Finance, derivatives from B3 (main brazilian exchange) and interest rate futures from the CME.

Most of the scripts and databases used are not provided in this repository, so it only serves to study the logic used and to help you build your own fund. In this repository you'll find the following scripts:

/order_slips/commit.py -> gets order slips sent by the traders during the day, checks if all the necessary tickers fetched from Yahoo Finance are mapped and sends the trades to the database. 

/routines/exposure.py -> the main script in the whole fund. It collects all trades, makes the necessary cash adjustments (to account for bought and sold assets and daily settlements received and paid from futures) and consolidates each trader's position for the day. In summary, it adds the previous day's portfolio and the current day's trades to find the current day's portfolio. 

/giraldi_backend.py and /giraldi_futures.py are libraries I made. _backend.py contains standard library imports and database connections used in my script. _futures.py is mostly used by exposure.py and has all the functions needed to calculate daily futures settlements.

/attribution_report.py generates tables used in a performance attribution report. It breaks down trader performance by asset and asset class for given start and end dates. Also makes a graph showing the fund's performance

/exposure_report.py generates tables that show the trader's fx eposure and current positions. Essential for each person to know what their portfolios look like. 

Web scrapers and database acesses are not included.
