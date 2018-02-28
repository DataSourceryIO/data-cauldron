# DataSourcery.io Data Cauldron for .NET SDK (0.0.1)

The DataSourcery.io Data Cauldron for .NET SDK allows you to build applications that consume,
validate and merge data from multiple heterogenous data sources on a continuous basis.

## Features

- Supported Data Sources
    - JSON files
    - Microsoft SQL Server (starting with 2016)
    - Azure SQL Database
- Extract Data
    - Configure SQL statements to execute on local server and linked server objects to extract data
    - Read/write watermark values that can be used with SQL statements to capture incremental changes to data
    - Configure retry policy for handling transient network failure conditions
- Validate Data
    - Configure SQL statements to execute on local server and linked server objects to validate data
- Merge Data
    - Use partition keys, row keys and watermarks to apply changes to a homogenous data model
- Deployment
    - Package as IoT Edge Module to collect data from any location

## Versioning Information

- The Data Cauldron Client Library uses [the semantic versioning scheme.](http://semver.org/)

## Download & Install

### Via Git

To get the source code of the SDK via git just type:

```bash
git clone git://github.com/DataSourceryIO/data-cauldron.git
cd data-cauldron
```

## Dependencies

### Newtonsoft Json

The desktop and phone libraries depend on Newtonsoft Json, which can be downloaded directly or referenced by your code project through Nuget.

- [Newtonsoft.Json] (http://www.nuget.org/packages/Newtonsoft.Json)

## Collaborate & Contribute

We gladly accept community contributions.

- Issues: Please report bugs using the Issues section of GitHub
- Forums: Interact with the development teams on StackOverflow
- Source Code Contributions: Please see [CONTRIBUTING.md](CONTRIBUTING.md) for instructions on how to contribute code.
