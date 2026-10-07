const DRIVE_FOLDER_MIME = "application/vnd.google-apps.folder";

export function assertWritableSharedFolder(folder, expectedId) {
  if (folder?.id !== expectedId || folder.mimeType !== DRIVE_FOLDER_MIME || folder.trashed) {
    throw new Error("Google-Ziel ist kein aktiver, eindeutig verifizierter Ordner.");
  }
  if (folder.capabilities?.canAddChildren !== true) {
    throw new Error("Das aktive Google-Konto darf in diesem Ordner keine Dateien anlegen.");
  }
}

export async function resolveCopyDestination({ targetFolderId, isAllowlisted, authorizeMoritz, readFolder }) {
  if (await isAllowlisted(targetFolderId)) {
    return { scope: "agent_folder", authorization: null };
  }
  const authorization = await authorizeMoritz();
  const folder = await readFolder(targetFolderId);
  assertWritableSharedFolder(folder, targetFolderId);
  return { scope: "moritz_authorized_shared_folder", authorization };
}

export function assertInstructionReferencesDriveIds(instruction, ids) {
  const text = String(instruction || "");
  const missing = ids.filter((id) => !text.includes(id));
  if (missing.length) {
    throw new Error(`Der aktuelle Asana-Auftrag nennt diese Drive-ID(s) nicht: ${missing.join(", ")}`);
  }
}

export function asanaDriveInstructionText(task, authorizationStory) {
  const parts = authorizationStory
    ? [authorizationStory.text, authorizationStory.html_text]
    : [task?.name, task?.notes, task?.html_notes];
  return parts.filter(Boolean).join("\n");
}

export function assertRecoverableTrashTarget(file, { fileId, expectedName, expectedParentId, expectedModifiedTime }) {
  if (file?.id !== fileId || file.mimeType === DRIVE_FOLDER_MIME || file.trashed) {
    throw new Error("Nur eine aktive, eindeutig verifizierte Datei kann in den Papierkorb verschoben werden.");
  }
  if (file.name !== expectedName || !file.parents?.includes(expectedParentId)) {
    throw new Error("Dateiname oder Elternordner haben sich geändert; Vorgang neu prüfen.");
  }
  if (expectedModifiedTime && file.modifiedTime !== expectedModifiedTime) {
    throw new Error("Die Datei wurde seit der Vorschau geändert; Vorgang neu prüfen.");
  }
  if (file.capabilities?.canTrash !== true) {
    throw new Error("Das aktive Google-Konto darf diese Datei nicht in den Papierkorb verschieben.");
  }
}
