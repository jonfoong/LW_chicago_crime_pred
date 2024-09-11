# LW_chicago_crime_pred

Predicting crime trends is inherently challenging, even when leveraging robust statistical models grounded in historical data. Some factors that influence crime, such as the day of the week or month, weather patterns, and holidays, can be anticipated with relative precision. However, many others remain unpredictable. Few, if any, foresaw the COVID-19 pandemic, the nationwide protests following George Floyd’s murder, or the recent surge in inflation, events that have had profound impacts on societal behavior and, consequently, on crime rates.

In our analysis, we explored various time units to predict crime trends, examining the number of crimes per day, week and month. We applied different models, including ARIMA and SARIMA and eventually moving to a Deep Learning models to identify the most significant patterns. Ultimately, the most meaningful results were found in daily crime data, which became the focus of our analysis.

Despite our efforts to use variables such as weather conditions, holidays, and weekdays, these factors did not significantly alter our predictions. This led us to refocus on the number of crimes and community areas, where more consistent patterns emerged. It is likely that with a smaller dataset (we used data from 2001 to present) the aforementioned external factors would be needed to achieve a similar performance.
