Reflow Configuration

Configuration in reflow is implemented using the infra (github.com/grailbio/infra) package.

Infra configuration expects: 
- a schema that defines the infrastructure schema and
- schema keys that specifies the providers that satisfy the schema.

Each provider can choose to implement zero or more of the following methods, based on what it needs to do.
A provider can depend on other providers to initialize or setup itself as long as there are no cyclic dependencies.
- `Init(p1 Provider1, p2 Provider2, ...) error`
- `Setup(pa ProviderA, pb Providerb, ...) error`
- `InstanceConfig() interface{}`
- `Config() interface{}`
- `Version() int`

