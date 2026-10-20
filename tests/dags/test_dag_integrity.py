def test_dagbag_has_no_import_errors(dagbag):
    assert dagbag.import_errors == {}
