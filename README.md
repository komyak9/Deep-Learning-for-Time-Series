## Overview
<p align="full-width">This pet-project aims to compare a classical Machine Learning model, LightGBM, with Neural Networks for the time-series forecasting task in terms of dev time complexity. The task is to create several models for the day-ahead energy price forecasting using open source data for Germany, while tracking the time spent on each model with Clockify.</p>
<p align="full-width">The project is designed to ensure all experiments are reproducible.</p>


## Structure
```
Deep-Learning-for-Time-Series/
├── data/
│   └── raw/
│       ├── capacities/                  # installed capacity of power generating units per energy type
│       ├── consumption/                 # energy load
│       ├── prices/                      # day-ahead energy prices
│       ├── production/                  # energy production  
│       └── weather_forecast/            # historical weather forecast
│   └── preprocessed/               
├── models/
├── notebooks/
│   ├── 01_data_exploration.ipynb        # EDA
│   ├── 02_feature_engineering.ipynb     # feature engineering & selection
├── src/
│   ├── load_data.py
│   ├── data_preprocessing.py            # scripts for data cleaning & transformation
├── requirements.txt
├── .gitignore
└── README.md
```
