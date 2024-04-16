# Delivery Command

`delivery` command offers a comprehensive overview of properties associated with the account. This information includes group ID, contract ID, version details (latest, staging, production), last update timestamp, product ID, hostname count, and a list of hostnames.

![delivery](./help/delivery.jpg)

`--summary` argument provides a structural overview of [Property Groups structures](https://control.akamai.com/apps/property-manager/#/groups) and the count of properties within each group. Unlike the order displayed on Akamai Control Center portal, CLI organizes groups and properties alphabetically.

```bash
akamai util delivery --summary --show
```

For detailed property information, the `--summary` argument can be omitted. Additionally, the `--behavior` argument facilitates checks for specific behaviors implemented by properties. Multiple behaviors are supported.

```bash
akamai util delivery --show --behavior adaptiveAcceleration allowPost \
    cacheKeyIgnoreCase caching cpCode downstreamCache enhancedAkamaiProtocol \
    failAction gzipResponse http2 imageManager mPulse \
    modifyOutgoingResponseHeader origin prefetch prefetchable redirect report \
    setVariable siteShield sureRoute
```

`delivery` command features seven subcommands:

- [behavior](#delivery-behavior)
- [custom-behavior](#delivery-custom-behavior)
- [metadata](#delivery-metadata)
- [hostname-cert](#delivery-hostname-cert)
- [origin-cert](#delivery-origin-cert)
- [ruletree](#delivery-ruletree)
- [activate]()

detailed arguments for each subcommand can be accessed using the `-h/--help` option:

```bash
akamai util delivery [subcommand] -h
```

## Delivery Behavior

To view all behaviors associated with a property

```bash
akamai util delivery behavior --property sample
```

## Delivery Custom-Behavior

To retrieve a list of all custom behaviors on the account

```bash
akamai util delivery custom-behavior
akamai util delivery custom-behavior --hidexml
akamai util delivery custom-behavior --id cbe_382209004 cbe_314399535
akamai util delivery custom-behavior --id cbe_382209004 cbe_314399535 --lineno
akamai util delivery custom-behavior --namecontains IPA --hidexml
```

## Delivery Metadata

To get advanced criteria, advanced behavior and advanced override on a property. Multiple properties are supported.

```bash
akamai util delivery metadata
akamai util delivery metadata --property SANDBOX SANDBOX_2
akamai util delivery metadata --property SANDBOX --hidexml
akamai util delivery metadata --property SANDBOX --lineno
```

## Delivery Hostname-Cert

To obtain a list of hostnames and corresponding edge hostnames on a property and whether they are CPS managed or Secure By Default. Multiple properties are supported.

```bash
akamai util delivery hostname-cert --property A B C D
```

## Delivery Netstorage

To get netstorage details utilized by properties on the account. Optionally, you can filter by groupId or by property.

```bash
akamai util delivery netstorage --concurrency 10
akamai util delivery netstorage --group-id 11111 11112 11113 --concurency 5
akamai util delivery netstorage --property A B C D
```

## Delivery Origin-Cert

To retrieve certificate information for all origins on a property
Note that properties implement Site Shield are exempt due to limitations. The command offers Site Shield map details and a list of CIDR and IPs.

```bash
akamai util delivery origin-cert
akamai util delivery origin-cert  --group-id 11111 11112 11113
akamai util delivery origin-cert  --property A B C D
```

## Delivery Ruletree

To obtain a hierarchical representation of a property's ruletree structure. For deep nested rules, utilize the `--show-depth` argument to identify the highest depth. The `--show-limit` argument reveals other delivery configuration limits.
