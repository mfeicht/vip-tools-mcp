import assert from "node:assert/strict";

import { selectImapUidWindow, sortImapMessagesByUidWindow } from "../lib/imap-window.js";

const uids = ["1", "2", "10", "4", "10", "invalid"];

assert.deepEqual(selectImapUidWindow(uids, { limit: 3 }), ["10", "4", "2"]);
assert.deepEqual(
  selectImapUidWindow(uids, { limit: 2, offset: 1, order: "newest_first" }),
  ["4", "2"]
);
assert.deepEqual(
  selectImapUidWindow(uids, { limit: 3, order: "oldest_first" }),
  ["1", "2", "4"]
);
assert.deepEqual(
  sortImapMessagesByUidWindow(
    [{ uid: "2" }, { uid: "10" }, { uid: "4" }],
    ["10", "4", "2"]
  ).map((message) => message.uid),
  ["10", "4", "2"]
);

console.log("imap-window-selftest: ok");
