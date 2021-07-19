package assoc

import "flag"

type AssocFlagsTrait struct {
	TableName string
}

func (a *AssocFlagsTrait) Flags(flags *flag.FlagSet) {
	flags.StringVar(&a.TableName, "table", "", "name of the dynamodb table")
}
