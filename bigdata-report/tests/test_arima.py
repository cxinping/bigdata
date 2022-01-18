# -*- coding: utf-8 -*-
from pyramid.arima import auto_arima

import pyramid as pm

# Create an array like you would in R
x = pm.c(1, 2, 3, 4, 5, 6, 7)

# Compute an auto-correlation like you would in R:
pm.acf(x)

# Plot an auto-correlation:
pm.plot_acf(x)