from syft_rds.client.rds_client import init_session


def test_dataset_create(do_syftbox_config, ds_syftbox_config):
    # import pdb; pdb.set_trace()
    do_rds_client = init_session(
        host=do_syftbox_config.email, syftbox_client_config_path=do_syftbox_config.path
    )
    ds_rds_client = init_session(
        host=do_syftbox_config.email, syftbox_client_config_path=ds_syftbox_config.path
    )
    assert ds_rds_client.host == do_rds_client.email
    assert do_rds_client.is_admin
    assert not ds_rds_client.is_admin
