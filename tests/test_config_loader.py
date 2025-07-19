# tests/test_config_loader.py
from src.py.anquant.util.config_loader import load_config, load_symbol_mappings, load_credentials


def test_config_loader():
    config = load_config("config/config.yaml")
    assert 'global' in config
    assert 'markets' in config['global']
    assert 'india' in config['global']['markets']

    mappings = load_symbol_mappings(config, 'india', 'angelone')
    assert isinstance(mappings, dict)

    credentials = load_credentials(config, 'india', 'angelone')
    assert isinstance(credentials, dict)


if __name__ == "__main__":
    test_config_loader()