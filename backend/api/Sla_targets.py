# api/sla_targets.py

# Existing links (HOURS)
EXISTING_TARGET_HOURS = {
    "termination": 3,
    "upgradation simple": 3,
    "upgradation with changes": 7,
    "downgradation": 3,
}

# New Link Provisioning rules
CUSTOMER_OWN_TARGET_HOURS = 16

# FTTH (DAYS)
FTTH_TARGET_DAYS = 3

# Vendor New Link targets (DAYS) based on Wired/Wireless column
VENDOR_NEWLINK_TARGET_DAYS = {
    "wired": 10,
    "wireless": 5,
}
