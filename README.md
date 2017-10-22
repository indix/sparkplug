# ![Sparkplug](./logo/logo.png)
Spark package to "plug" holes in data using SQL based rules.

## Motivation

At Indix, we work with a lot of data and our data pipeline runs bunch of ML alogs on our data. There are cases where we have to "plug" or override certain values or predictions in our data. This maybe because of bugs or deficiencies in our current models. 

Sparkplug is a rule based override system that helps us to do fixes in our data. The rules also act as central place of tracking "debt" that we need to track while doing improvements to our aglorithms and models.
