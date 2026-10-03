import assert from "node:assert/strict";
import test from "node:test";

import {
  buildGeneratedCloudinaryLocation,
  detectGeneratedImageMimeType,
  readGeneratedImageDimensions,
  validateGeneratedDriveImageMetadata,
  verifyGeneratedImageBytes
} from "./generated-image-handoff.js";

const jpeg = Buffer.from([0xff, 0xd8, 0x01, 0x02, 0x03, 0x04, 0xff, 0xd9]);
const png = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x01]);

test("detects and verifies supported generated image bytes", () => {
  assert.equal(detectGeneratedImageMimeType(jpeg), "image/jpeg");
  assert.equal(detectGeneratedImageMimeType(png), "image/png");
  assert.equal(verifyGeneratedImageBytes(jpeg, "image/jpeg").sha256.length, 64);
  assert.throws(() => verifyGeneratedImageBytes(png, "image/jpeg"), /widerspricht/);
});

test("reads JPEG and PNG dimensions for batch preflight", () => {
  const jpegWithSize = Buffer.from([
    0xff, 0xd8, 0xff, 0xe0, 0x00, 0x04, 0x00, 0x00,
    0xff, 0xc0, 0x00, 0x11, 0x08, 0x09, 0x00, 0x07, 0x40,
    0x03, 0x01, 0x11, 0x00, 0x02, 0x11, 0x00, 0x03, 0x11,
    0xff, 0xd9
  ]);
  const pngWithSize = Buffer.concat([Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0, 0, 0, 13]), Buffer.from("IHDR"), Buffer.from([0, 0, 8, 0, 0, 0, 8, 0])]);
  assert.deepEqual(readGeneratedImageDimensions(jpegWithSize, "image/jpeg"), { width: 1856, height: 2304 });
  assert.deepEqual(readGeneratedImageDimensions(pngWithSize, "image/png"), { width: 2048, height: 2048 });
  assert.equal(readGeneratedImageDimensions(jpeg, "image/jpeg"), null);
});

test("requires Gemini provenance, image metadata and an allowed parent candidate", () => {
  const result = validateGeneratedDriveImageMetadata(
    {
      mimeType: "image/jpeg",
      size: "1234",
      parents: ["folder-1"],
      appProperties: { provider: "google-gemini" }
    },
    2_000
  );
  assert.equal(result.mimeType, "image/jpeg");
  assert.throws(
    () => validateGeneratedDriveImageMetadata({ mimeType: "image/jpeg", size: "1234", parents: ["folder-1"] }, 2_000),
    /nicht als Google-Gemini-Ausgabe/
  );
  assert.throws(
    () =>
      validateGeneratedDriveImageMetadata(
        { mimeType: "image/jpeg", size: "3000", parents: ["folder-1"], appProperties: { provider: "google-gemini" } },
        2_000
      ),
    /groesser als das Limit/
  );
});

test("builds stable project-scoped Cloudinary paths", () => {
  const sha256 = "a".repeat(64);
  const first = buildGeneratedCloudinaryLocation({
    folderPrefix: "vip-social-media",
    projectKey: "TradingPulse",
    assetKey: "2026-10-02 Slide 1",
    fileName: "BofA visual.jpg",
    index: 0,
    sha256
  });
  const second = buildGeneratedCloudinaryLocation({
    folderPrefix: "vip-social-media",
    projectKey: "TradingPulse",
    assetKey: "2026-10-02 Slide 1",
    fileName: "BofA visual.jpg",
    index: 0,
    sha256
  });
  assert.deepEqual(first, second);
  assert.equal(first.folder, "vip-social-media/tradingpulse/generated-2026-10-02-slide-1");
  assert.equal(first.publicId, "01-bofa-visual-aaaaaaaaaaaa");
});
