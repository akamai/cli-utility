# cli-utility

Provides a collection of useful tools.

# local install

Depending on your local python setup,
replace `python with python3` and `pip with pip3`

```
git pull https://github.com/akamai/cli-utility
cd cli-utility
pre-commit install
python -m venv .venv
pip install --upgrade pip
pip install -r requirements.txt
```

# cheatsheet command

```
python bin/akamai-utility.py -h

# administrative
python bin/akamai-utility.py search --account "akamai internal" "direct customer"
python bin/akamai-utility.py --section GOV search --account "akamai internal" "direct customer"

# compare delivery/security config
python bin/akamai-utility.py -a $account diff --config1 $config1 --left $left --right $right
python bin/akamai-utility.py -a $account diff --config1 $config1 --security
python bin/akamai-utility.py -a $account diff --config1 $config1 --left $config1_version --config2 $config2 --right $config2_version
python bin/akamai-utility.py -a $account diff --config1 $config1 --left $config1_version --config2 $config2 --right $config2_version --xml --no-show

# miscelleanouse tools about delivery config
python bin/akamai-utility.py -a $account delivery-config -h
python bin/akamai-utility.py -a $account delivery-config --show
python bin/akamai-utility.py -a $account delivery-config --show --group-id $group1 $group2
python bin/akamai-utility.py -a AANA-2NUHEA delivery-config --advancedmetadata --property-id 743088 670262 --version 10
python bin/akamai-utility.py -a $account delivery-config --ruletree --property-id 861132 --version 1 --show

# reports
python bin/akamai-utility.py -a $account report

# ruleformat catalog
python bin/akamai-utility.py ruleformat --product-id prd_SPM --version latest --behavior cache web --json
python bin/akamai-utility.py ruleformat --product-id prd_SPM --version v2020-11-02 --behavior cache web --xl

# convert ghost log into excel
python bin/akamai-utility.py log --input sample.gz --output sample.xlsx

```
