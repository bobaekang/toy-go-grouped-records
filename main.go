package main

func main() {
	// check if Table implements Table interfaces at complie time
	var _ TableDataService = (*Table)(nil)
	var _ TableFetcher = (*Table)(nil)
	var _ TableJSONService = (*Table)(nil)
	var _ TablePrinter = (*Table)(nil)
}
