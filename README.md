# ![Sparkplug](./logo/logo.png)
Spark package to "plug" holes in data using SQL based rules.

## Motivation

At Indix, we work with a lot of data and our data pipeline runs bunch of ML alogs on our data. There are cases where we have to "plug" or override certain values or predictions in our data. This maybe because of bugs or deficiencies in our current models. 

Sparkplug is a rule based override system that helps us to do fixes in our data. The rules also act as central place of tracking "debt" that we need to track while doing improvements to our aglorithms and models.

## Design

We came up with a system that enables engineering, customer success and product management to make the necessary fixes in the data. Using rules based on SQL conditions (WHERE clause predicate), we provided a way to override and fix values / predictions in our data. Each rule has the condition along with the fields that are to be overridden with their respective override values.

Sparkplug is the core of this system. We also have an internal GUI based tool that utilizes our internal data pipeline platform to sample data, apply the rules and provide detailed view into how each rule affects the data.

Sparkplug leverages Spark-SQL to do much of its work. Sparkplug is designed to run within our internal data pipeline platform as well as standalone Spark jobs.

## Getting Started

Let's first talk about how rules that Sparkplug works with look like.

### Rules

An example rule is given below in json:

```json
{
  "name": "rule1",
  "condition": "title like '%iPhone%'",
  "actions": [
    {
      "key": "title",
      "value": "Apple iPhone"
    }
  ]
}
```
Each rule identifies itself with a `name`. The SQL predicate in `condition` is used to identify the applicable rows in the data. On the selected rows, the `actions` - specified via the column name in `key` and its overridden `value` - are applied to plug the data.
