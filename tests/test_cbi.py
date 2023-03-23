import filecmp
from os.path import join

import pytest
from cbi import start
from hdx.api.configuration import Configuration
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent


class TestCBI:
    @pytest.fixture(scope="function")
    def configuration(self):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join("config", "project_configuration.yml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="function")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    def test_cbi(self, configuration, fixtures_dir):
        with temp_dir(
            "TestCBIViz", delete_on_success=True, delete_on_failure=False
        ) as temp_folder:
            with Download() as downloader:
                with Retrieve(
                    downloader,
                    temp_folder,
                    join(fixtures_dir, "input"),
                    temp_folder,
                    save=False,
                    use_saved=True,
                ) as retriever:
                    today = "2022-06-02"
                    start(configuration, today, retriever, temp_folder, "ukraine")
                for filename in (
                    "flows",
                    "transactions",
                    "reporting_orgs",
                    "receiver_orgs",
                ):
                    csv_filename = f"{filename}.csv"
                    expected_file = join(fixtures_dir, csv_filename)
                    actual_file = join(temp_folder, csv_filename)
                    assert_files_same(expected_file, actual_file)
                    json_filename = f"{filename}.json"
                    expected_file = join(fixtures_dir, json_filename)
                    actual_file = join(temp_folder, json_filename)
                    assert filecmp.cmp(expected_file, actual_file)
