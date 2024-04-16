# Ruleformat

The `ruleformat` command provides JSON format from delivery configurations that match a given product ID and rule format version.
This JSON is valuable for referencing during activation issues.
The command also aids in obtaining JSON schema and snippet samples for specific behaviors.

![ruleformat](./help/ruleformat.jpg)

```bash
# Retrieve all behaviors of prd_SPM's latest version in xlsx format
akamai util ruleformat --product-id prd_SPM --version latest --xlsx

# Filter behavior containing the keyword "origin"
akamai util ruleformat --product-id prd_SPM --version latest \
    --behavior origin

# Obtain JSON schema and sample for behavior containing the keyword "conditionalOrigin"
akamai util ruleformat --product-id prd_SPM --version latest \
    --behavior conditionalOrigin --json --sample

# Retrieve JSON schema in both JSON and table formats, along with a snippet sample
akamai util ruleformat --product-id prd_SPM --version latest \
    --behavior conditionalOrigin --json --sample --table
```
