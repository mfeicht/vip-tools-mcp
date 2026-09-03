import assert from "node:assert/strict";

import {
  selectImapUidPage,
  selectImapUidWindow,
  sortImapMessagesByUidWindow
} from "../lib/imap-window.js";

const uids = ["1", "2", "10", "4", "10", "invalid"];

assert.deepEqual(selectImapUidWindow(uids, { limit: 3 }), ["10", "4", "2"]);
assert.deepEqual(
  selectImapUidWindow(uids, { limit: 2, offset: 1, order: "newest_first" }),
  ["4", "2"]
);

assert.deepEqual(
  selectImapUidPage(["1", "2", "4", "5", "23", "101", "102"], {
    limit: 2,
    order: "oldest_first",
    excludeUids: ["4", "5", "23"]
  }),
  {
    uids: ["1", "2"],
    order: "oldest_first",
    cursor_uid: null,
    last_uid: "2",
    next_cursor_uid: "2",
    has_more: true,
    remaining_count: 2
  }
);
assert.deepEqual(
  selectImapUidPage(["1", "2", "4", "5", "23", "101", "102"], {
    limit: 2,
    cursorUid: "2",
    order: "oldest_first",
    excludeUids: ["4", "5", "23"]
  }).uids,
  ["101", "102"]
);
assert.deepEqual(
  selectImapUidPage(["1", "2", "4", "5", "23", "101", "102"], {
    limit: 2,
    cursorUid: "101",
    order: "newest_first",
    excludeUids: ["4", "5", "23"]
  }).uids,
  ["2", "1"]
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
