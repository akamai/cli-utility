# cli-utility

Provides a collection of useful tools.

# Installation

```bash
akamai install utility
```

# Usage

Use `-h/--help`- to get a list of available top commands via akamai utility CLI

```bash
akamai utility -h
akamai utility --help
akamai util -h
akamai util --help
```

![main help](./bin/doc/help/utility.jpg)

# delivery

You can use `delivery` command to get high level information about all properties on the account. Those information includes groupId, contractId, versions (latest, staging, production), last updated, productId, # of hostnames, list of hostnames.

![delivery](./bin/doc/help/delivery.jpg)

`delivery` command has 7 subcommands: `behavior`, `custom-behavior`, `metadata`, `hostname-cert`, `origin-cert`, `ruletree`, and `activate`.

Each summand has its own arguments, use `-h/--help` for completion

```bash
akamai util delivery [subcommand] -h
```

# security

You can use `security` command to download one or more security configurations on the account. If the security configuration has network list, you will have a list of IP addresses included in the result.

`security` command has one subcommand `hostname` which provides a list of hostname activating on Akamai staging network not yet added to the security config.

![security](./bin/doc/help/security.jpg)

# diff

`diff` command supports both delivery and security configuration, both JSON and XML format. You can also compare 2 different delivery configurations.

# ruleformat

`ruleformat` provides JSON format from delivery configuration that match productId and rule format version. This JSON is helpful for referencing for example when you run into activation issue. The command also provide other arguments that help you narrow down on specific behaviors. This is also convenient if you are using Akamai-As-Code as you can reference and get sample JSON snippet for specific behaviors.

![ruleformat](./bin/doc/help/ruleformat.jpg)

###sample commands

```bash
akamai util ruleformat --product-id prd_SPM --version latest --xlsx

# lookup behavior contains keyword "origin"
akamai util ruleformat --product-id prd_SPM --version latest --behavior origin

# lookup behavior contains keyword "conditionalOrigin", display json schema and provide snippet sample
akamai util ruleformat --product-id prd_SPM --version latest --behavior conditionalOrigin --json --sample

# lookup behavior contains keyword "conditionalOrigin", display json schema in json format and table format, and provide snippet sample
akamai util ruleformat --product-id prd_SPM --version latest --behavior conditionalOrigin --json --sample --table

```

# Contribution

By submitting a contribution (the “Contribution”) to this project, and for good and valuable consideration, the receipt and sufficiency of which are hereby acknowledged, you (the “Assignor”) irrevocably convey, transfer, and assign the Contribution to the owner of the repository (the “Assignee”), and the Assignee hereby accepts, all of your right, title, and interest in and to the Contribution along with all associated copyrights, copyright registrations, and/or applications for registration and all issuances, extensions and renewals thereof (collectively, the “Assigned Copyrights”). You also assign all of your rights of any kind whatsoever accruing under the Assigned Copyrights provided by applicable law of any jurisdiction, by international treaties and conventions and otherwise throughout the world.

## Local Install

Depending on your local python setup,
replace `python with python3` and `pip with pip3`

```
git clone https://github.com/akamai/cli-utility
cd cli-utility
pre-commit install
git checkout -b [branchname]
python -m venv .venv
pip install --upgrade pip
pip install -r requirements.txt
```

# Notice

Copyright 2023 – Akamai Technologies, Inc.

All works contained in this repository, excepting those explicitly otherwise labeled, are the property of Akamai Technologies, Inc.
