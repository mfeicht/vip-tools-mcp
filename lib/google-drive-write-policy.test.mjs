import assert from "node:assert/strict";
import test from "node:test";
import { assertInstructionReferencesDriveIds, assertRecoverableTrashTarget, assertWritableSharedFolder, resolveCopyDestination } from "./google-drive-write-policy.mjs";

const folder = {
  id: "target",
  mimeType: "application/vnd.google-apps.folder",
  trashed: false,
  capabilities: { canAddChildren: true }
};

test("shared target requires the exact active folder and Google write capability", () => {
  assert.doesNotThrow(() => assertWritableSharedFolder(folder, "target"));
  assert.throws(() => assertWritableSharedFolder({ ...folder, id: "other" }, "target"));
  assert.throws(() => assertWritableSharedFolder({ ...folder, trashed: true }, "target"));
  assert.throws(() => assertWritableSharedFolder({ ...folder, capabilities: { canAddChildren: false } }, "target"));
  assert.throws(() => assertWritableSharedFolder({ ...folder, capabilities: {} }, "target"));
});

test("allowlisted copy target does not require the expanded permission path", async () => {
  const result = await resolveCopyDestination({
    targetFolderId: "target",
    isAllowlisted: async () => true,
    authorizeMoritz: async () => { throw new Error("unexpected authorization"); },
    readFolder: async () => { throw new Error("unexpected folder read"); }
  });
  assert.deepEqual(result, { scope: "agent_folder", authorization: null });
});

test("shared copy target requires Moritz authorization before checking Google write access", async () => {
  let folderRead = false;
  await assert.rejects(resolveCopyDestination({
    targetFolderId: "target",
    isAllowlisted: async () => false,
    authorizeMoritz: async () => { throw new Error("not Moritz-authorized"); },
    readFolder: async () => { folderRead = true; return folder; }
  }), /not Moritz-authorized/);
  assert.equal(folderRead, false);

  const result = await resolveCopyDestination({
    targetFolderId: "target",
    isAllowlisted: async () => false,
    authorizeMoritz: async () => ({ source: "asana", require_moritz: true }),
    readFolder: async () => folder
  });
  assert.equal(result.scope, "moritz_authorized_shared_folder");
  assert.equal(result.authorization.require_moritz, true);
});

test("Asana resource scope must name every exact Drive ID", () => {
  assert.doesNotThrow(() => assertInstructionReferencesDriveIds("Quelle src123; Ziel dst456", ["src123", "dst456"]));
  assert.throws(() => assertInstructionReferencesDriveIds("Quelle src123", ["src123", "dst456"]), /dst456/);
});

const file = {
  id: "file",
  name: "Report.xlsx",
  mimeType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  parents: ["target"],
  modifiedTime: "2026-10-07T08:00:00Z",
  trashed: false,
  capabilities: { canTrash: true }
};
const expected = {
  fileId: "file",
  expectedName: "Report.xlsx",
  expectedParentId: "target",
  expectedModifiedTime: "2026-10-07T08:00:00Z"
};

test("trash target rejects folders, stale identity, stale revision and missing capability", () => {
  assert.doesNotThrow(() => assertRecoverableTrashTarget(file, expected));
  assert.throws(() => assertRecoverableTrashTarget({ ...file, id: "other" }, expected));
  assert.throws(() => assertRecoverableTrashTarget({ ...file, name: "Other.xlsx" }, expected));
  assert.throws(() => assertRecoverableTrashTarget({ ...file, parents: ["other"] }, expected));
  assert.throws(() => assertRecoverableTrashTarget({ ...file, modifiedTime: "later" }, expected));
  assert.throws(() => assertRecoverableTrashTarget({ ...file, mimeType: folder.mimeType }, expected));
  assert.throws(() => assertRecoverableTrashTarget({ ...file, capabilities: { canTrash: false } }, expected));
});
