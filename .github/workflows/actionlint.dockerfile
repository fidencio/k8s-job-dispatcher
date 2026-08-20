# Copyright (c) 2026 Kata Contributors
#
# SPDX-License-Identifier: Apache-2.0
#
# Never built. It is where dependabot can see the actionlint the workflow runs,
# since it bumps neither a `uses: docker://` reference nor an image named in a
# `run:`.

FROM rhysd/actionlint:1.7.12@sha256:b1934ee5f1c509618f2508e6eb47ee0d3520686341fec936f3b79331f9315667
