def test_valid(cldf_dataset, cldf_logger):
    assert cldf_dataset.validate(log=cldf_logger)


def test_forms(cldf_dataset):
    assert len(list(cldf_dataset["FormTable"])) == 44876
    assert any(f["Form"] == "boo-row-a" for f in cldf_dataset["FormTable"])


def test_parameters(cldf_dataset):
    assert len(list(cldf_dataset["ParameterTable"])) == 344


def test_languages(cldf_dataset):
    assert len(list(cldf_dataset["LanguageTable"])) == 190


def test_cognates(cldf_dataset):
    assert len(list(cldf_dataset["CognateTable"])) == 44876
    assert any(f["Form"] == "arra" for f in cldf_dataset["CognateTable"])
