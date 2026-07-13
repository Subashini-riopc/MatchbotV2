"""Snowflake-native demo pipeline glue for MatchBot.

This package generates and executes SQL against Snowflake; it never loops
over staged records in Python. It depends on the core ``matchbot`` package
(never the reverse) to reuse config loading, matcher-chain resolution, and
matching-attribute constants — so ``config/global.yaml`` and
``config/providers/*.yaml`` remain the single source of truth for both the
AWS and Snowflake demos. See ``docs/snowflake-implementation-plan.md`` for
the full design.
"""

from __future__ import annotations
