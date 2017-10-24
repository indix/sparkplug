# ![Sparkplug](./logo/logo.svg)
Spark package to "plug" holes in data using SQL based rules. 

[![Build Status](https://travis-ci.org/indix/sparkplug.svg?branch=master)](https://travis-ci.org/indix/sparkplug) [![Maven](https://maven-badges.herokuapp.com/maven-central/com.indix/sparkplug_2.11/badge.svg)](http://repo1.maven.org/maven2/com/indix/sparkplug_2.11/)

## Motivation

At Indix, we work with a lot of data and our data pipeline runs bunch of ML algos on our data. There are cases where we have to "plug" or override certain values or predictions in our data. This maybe because of bugs or deficiencies in our current models. 

SparkPlug is a rule based override system that helps us to do fixes in our data. The rules also act as central place of tracking "debt" that we need to track while doing improvements to our aglorithms and models.

## Design

We came up with a system that enables engineering, customer success and product management to make the necessary fixes in the data. Using rules based on SQL conditions (WHERE clause predicate), we provided a way to override and fix values / predictions in our data. Each rule has the condition along with the fields that are to be overridden with their respective override values.

SparkPlug is the core of this system. We also have an internal GUI based tool that utilizes our internal data pipeline platform to sample data, apply the rules and provide detailed view into how each rule affects the data.

SparkPlug leverages Spark-SQL to do much of its work. SparkPlug is designed to run within our internal data pipeline platform as well as standalone Spark jobs.

## Getting Started

Let's first talk about how rules that SparkPlug works with look like.

### Rules

An example rule is given below in json:

```json
{
  "name": "rule1",
  "version": "version1",
  "condition": "title like '%iPhone%'",
  "actions": [
    {
      "key": "title",
      "value": "Apple iPhone"
    }
  ]
}
```
Each rule identifies itself with a `name`. The current version of the rule can be identified using `version`. The SQL predicate in `condition` is used to identify the applicable rows in the data. On the selected rows, the `actions` - specified via the column name in `key` and its overridden `value` - are applied to plug the data. The value is currently always specified as a string, and is internally validated and convereted to the appropriate type.

Rules can be fed into Sparkplug as a normal jsonlines dataset that Spark can work with.

Sparkplug comes with an helper to deserialize json rules into a collection of `PlugRule` objects, which is shown below:

```scala
// example of creating a Spark session
implicit val spark: SparkSession = SparkSession.builder
    .config(new SparkConf())
    .enableHiveSupport()
    .master("local[*]")
    .getOrCreate()
    
val rules = spark.readPlugRulesFrom(path)
```
The `rules` can now be fed into SparkPlug.

### Creating SparkPlug instance

SparkPlug comes with a builder that helps you instanstiate SparkPlug object with the right settings. The simple way to create one is as follows:

```scala
val sparkPlug = SparkPlug.builder.create()
```

Once we have the instance, we can "plug" a `DatFrame` with the rules:

```scala
sparkPlug.plug(df, rules)
```

### Rules validation

`SparkPlug.validate` method can be used to validate the input rules for a given schema.

```scala
sparkPlug.validate(df.schema, rules) // Returns a list of validation errors if any.
```

The SparkPlug object can also be created with validation enabled so that rules are validated before plugging:

```scala
val sparkPlug = SparkPlug.builder.enableRulesValidation.create()
```

### Plug details

To track what changes are being made (or not) to each record, it is possible to add `PlugDetails` to every record with information on which rules were applied to it. This is disabled by default and can be enables as follows:

```scala
val sparkPlug = SparkPlug.builder.enablePlugDetails.create()
```
This adds a `plugDetails` column of type `Seq[PlugDetail]` to the DataFrame. `PlugDetail` is a simple case class as defined below:

```scala
case class PlugDetail(name: String, version: String, fieldNames: Seq[String])
```

### Working with structs

It is possible to override values within a `StructType`. 


```json
{
  "name": "rule1",
  "version": "version1",
  "condition": "title like '%iPhone%'",
  "actions": [
    {
      "key": "price.min",
      "value": "100.0"
    },
    {
      "key": "price.max",
      "value": "1000.0"
    }
  ]
}
```

Currently SparkPlug supports only one level within structs.

### SQL in values

Values can be literal values like "iPhone", "100" or "999.9" etc. SparkPlug also allow SQL within values so that overrides can use the power of SQL and most importantly depend of values of other fields. Values enclosed within `` ` `` (backtick) are treated as SQL:


```json
{
  "name": "rule1",
  "version": "version1",
  "condition": "true",
  "actions": [
    {
      "key": "title",
      "value": "`concat(brand, ' ', title)`"
    }
  ]
}
```

The above rule appends the `brand` to the `title`
