![](https://github.com/gokurakujoudo/ouroboros/blob/master/Design/Logo-shade.png)

## Structure  

### data definition 

Dataset for running a back test is a collection of data frames, including categorical information and time series for one or many stocks, indices, and other market information. A extension of loading data from common data sources will be add later. Definitions are description for each data frame and offer a uniform interface for outside to filter and search data.

### strategy 

Strategy is a abstraction of ways to act and react to the market. A strategy itself can be a combination of many strategies. A strategy must has following components:

- Data definition of what data it will use. Including category, factor, and time period.
- One or more event function that will modify position along time and their running frequency.
- *Optional* parameters of the strategy that will be constant or can be produced in a constant way.

### back testing session 

Back test session is a back test run for a given time period given a strategy and relevant data. Data definition must match data. In the session, these component are required:

Before the run:
- Strategy
- Data

During the run:
- Schedule: a time table of event functions to be run during the entire period 
- Temporary position: position at the end of last adjustment
- Generated position moves: a time series of position move during event functions
- Generated internal moves: a time series records internal valuation or parameters change

After the run:
- Realized gain or loss for each asset along time

### infrastructure 

- data provider: a fast tool to be used for data filtering and chunking
- gain and loss calculator
- visualization tools

## Project Plan

### Stage 1 Make a swift workflow

Using testing dataset and simple strategy to create a runnable back test workflow

### Stage 2 Make a runnable package

Split functions into modules and classes and make a universal package

### Stage 3 Make a useful platform

Building better connection with data source provider and better result visualization
