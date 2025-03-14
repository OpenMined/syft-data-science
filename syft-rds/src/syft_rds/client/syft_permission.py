from syftbox.lib.permissions import ComputedPermission, SyftPermission
from syftbox.server.models.sync_models import RelativePath, AbsolutePath
from syft_core import Client
from syftbox.lib.constants import PERM_FILE
import yaml


def get_computed_permission(
    *,
    client: Client,
    path: RelativePath,
) -> "ComputedPermission":
    snapshot_folder = client.workspace.datasites
    # validate the paths

    path = RelativePath(path)
    snapshot_folder = AbsolutePath(snapshot_folder)

    # get all the rules
    all_rules = []
    for file in snapshot_folder.rglob(PERM_FILE):
        content = file.read_text()
        rule_dicts = yaml.safe_load(content)
        perm_file = SyftPermission.from_rule_dicts(
            permfile_file_path=file.relative_to(snapshot_folder), rule_dicts=rule_dicts
        )
        all_rules.extend(perm_file.rules)

    permission = ComputedPermission.from_user_rules_and_path(
        rules=all_rules, user=client.email, path=path
    )
    return permission
