  def test_schema (self):
        all_columns = set(self.expected.columns).union(set(self.tested.columns))
        getDataType = lambda df, series: df[series].dtype if series in df.columns else "Does not exist"
        self.schema_comparison = {
            "Column": list(all_columns),
            "Expected": [getDataType(self.expected, column) for column in all_columns],
            "Tested": [getDataType(self.tested, column) for column in all_columns],
        }
        self._result['schema_result'] = all(getDataType(self.expected, column) == getDataType(self.tested, column) for column in all_columns)
        if not self._result['schema_result']:
            self.__report__['schema_report'] = f"Expected and tested dataframes have different schemas.\n{self.schema_comparison}"
        return self._result['schema_result']
