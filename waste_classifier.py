"""Server-side waste classification.

Why this exists
---------------
The phone used to classify waste on-device with a bundled TFLite model. The
model and the app had to ship together, so fixing a bad class meant rebuilding
and reinstalling the APK on every phone. Running inference here means the model
can be replaced by copying one file.

Why ONNX and not TFLite
-----------------------
`tflite-runtime` publishes no Windows wheel, so it cannot be bundled into the
PyInstaller EXE that runs on the client's Windows machine. onnxruntime has a
Windows wheel, adds roughly 30 MB to the EXE, and was verified numerically
identical to the TFLite model (worst absolute difference 2.2e-06, 12/12
identical argmax) when the export was made.

What this module owns
---------------------
  * loading the model once, lazily, and only if the files exist
  * decoding and resizing an uploaded image exactly as training did
  * the confidence gates that turn raw probabilities into a decision

The gates are the same policy the Dart service used: a wrong bin is worse than
"not sure", so a low-margin prediction returns Unknown rather than a category.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any

import numpy as np

LOGGER = logging.getLogger(__name__)

IMAGE_SIZE = 224

# How long the ONNX session may sit unused before its memory is released.
IDLE_UNLOAD_SECONDS = int(os.getenv("WASTE_MODEL_IDLE_SECONDS", "60"))

# Confidence policy. Mirrors WasteDetectionService in the Flutter app, retuned
# for 6 classes:
#   * a waste category needs enough mass and a clear lead over the runner-up
#   * "Not Waste" needs to be decisive, otherwise the user gets no answer
#   * Unknown is a valid, safe result: it shows guidance but awards no points
MIN_CATEGORY_PROBABILITY = 0.40
MIN_CATEGORY_MARGIN = 0.08
MAX_NOT_WASTE_FOR_WASTE = 0.55
MIN_NOT_WASTE_PROBABILITY = 0.75

NOT_WASTE = "Not Waste"
UNKNOWN = "Unknown"

# Display mapping used by the app: Non-Biodegradable is shown as "Residual".
DISPLAY_NAMES = {
    "Biodegradable": "Biodegradable",
    "Recyclable": "Recyclable",
    "E-Waste": "E-Waste",
    "Non-Biodegradable": "Residual",
    "Textile": "Textile",
    "Not Waste": "Not Waste",
    UNKNOWN: "Not sure",
}

BIN_GUIDANCE = {
    "Biodegradable": "GREEN bin",
    "Recyclable": "BLUE bin",
    "E-Waste": "E-WASTE drop-off",
    "Non-Biodegradable": "BLACK bin",
    "Textile": "TEXTILE drop-off",
    "Not Waste": "",
    UNKNOWN: "",
}


class WasteClassifier:
    """Thin wrapper over an ONNX session with the eTrak decision policy."""

    def __init__(self, model_dir: Path):
        self.model_dir = Path(model_dir)
        self.model_path = self.model_dir / "waste_classifier.onnx"
        self.labels_path = self.model_dir / "labels.json"
        self._session = None
        self._input_name: str | None = None
        self._output_name: str | None = None
        self._labels: list[str] = []
        self._lock = threading.Lock()
        self._load_error: str | None = None
        self._last_used: float = 0.0
        self._reaper: threading.Timer | None = None

    # --------------------------------------------------------- idle unload #
    def touch(self) -> None:
        """Record a use and arm the idle-unload timer."""
        self._last_used = time.monotonic()
        if self._reaper is None or not self._reaper.is_alive():
            self._reaper = threading.Timer(IDLE_UNLOAD_SECONDS, self._unload_if_idle)
            self._reaper.daemon = True
            self._reaper.start()

    def _unload_if_idle(self) -> None:
        """Release the ONNX session after IDLE_UNLOAD_SECONDS of no scans.

        The model is ~9 MB of weights but ONNX Runtime holds several times that
        in arenas and thread pools, so an idle free-tier container keeps paying
        for memory nobody is using. Dropping the session returns it to the OS;
        the next scan reloads in a few hundred milliseconds.
        """
        with self._lock:
            if self._session is None:
                self._reaper = None
                return
            idle_for = time.monotonic() - self._last_used
            if idle_for < IDLE_UNLOAD_SECONDS:
                # A scan landed while the timer was firing; wait out the rest.
                self._reaper = threading.Timer(
                    IDLE_UNLOAD_SECONDS - idle_for, self._unload_if_idle
                )
                self._reaper.daemon = True
                self._reaper.start()
                return
            self._session = None
            self._input_name = None
            self._output_name = None
            self._reaper = None
        LOGGER.info(
            "Waste classifier unloaded after %ds idle (memory released)",
            IDLE_UNLOAD_SECONDS,
        )

    # ------------------------------------------------------------- loading #
    @property
    def available(self) -> bool:
        """True when the model files exist and are readable.

        Deliberately does NOT load the session: this backs a health endpoint the
        app polls, and loading here would defeat the idle unload by keeping the
        model resident forever. A load error recorded by a previous attempt
        still reports False, so a broken model stays diagnosable.
        """
        if self._session is not None:
            return True
        if self._load_error is not None:
            return False
        return self.model_path.exists() and self.labels_path.exists()

    @property
    def load_error(self) -> str | None:
        return self._load_error

    @property
    def labels(self) -> list[str]:
        return list(self._labels)

    def load(self) -> bool:
        """Load the ONNX session. Safe to call repeatedly.

        Loading is deferred so the API can start and serve every other endpoint
        even if the model is missing or ONNX Runtime is unavailable. A missing
        model must degrade to a clear error on the scan endpoint, never prevent
        the server from booting.
        """
        with self._lock:
            if self._session is not None:
                return True

            if not self.model_path.exists():
                self._load_error = f"model file missing: {self.model_path}"
                LOGGER.warning("Waste classifier disabled: %s", self._load_error)
                return False

            try:
                with self.labels_path.open(encoding="utf-8") as handle:
                    self._labels = [str(x) for x in json.load(handle)]
            except Exception as exc:  # noqa: BLE001
                self._load_error = f"labels.json unreadable: {exc}"
                LOGGER.warning("Waste classifier disabled: %s", self._load_error)
                return False

            if NOT_WASTE not in self._labels:
                self._load_error = "labels.json has no 'Not Waste' class"
                LOGGER.warning("Waste classifier disabled: %s", self._load_error)
                return False

            try:
                import onnxruntime as ort

                options = ort.SessionOptions()
                # One photo per request; extra threads only add latency under
                # the GIL for a model this small.
                options.intra_op_num_threads = 2
                options.inter_op_num_threads = 1
                options.log_severity_level = 3
                self._session = ort.InferenceSession(
                    str(self.model_path),
                    sess_options=options,
                    providers=["CPUExecutionProvider"],
                )
                self._input_name = self._session.get_inputs()[0].name
                self._output_name = self._session.get_outputs()[0].name
            except Exception as exc:  # noqa: BLE001
                self._load_error = f"onnxruntime failed to load the model: {exc}"
                LOGGER.warning("Waste classifier disabled: %s", self._load_error)
                self._session = None
                return False

            LOGGER.info(
                "Waste classifier ready: %d classes, input=%s output=%s",
                len(self._labels),
                self._input_name,
                self._output_name,
            )
            return True

    # ---------------------------------------------------------- inference #
    def _prepare(self, image_bytes: bytes) -> np.ndarray:
        """Decode bytes to the exact tensor layout training used.

        Training did: decode -> resize to 224x224 bilinear -> divide by 255.
        The ONNX graph contains the MobileNet rescaling to [-1, 1], so we must
        NOT rescale here; doing it twice would wreck the input distribution.
        """
        from PIL import Image
        import io

        with Image.open(io.BytesIO(image_bytes)) as img:
            img = img.convert("RGB")
            # BILINEAR matches tf.image.resize default used in training.
            img = img.resize((IMAGE_SIZE, IMAGE_SIZE), Image.BILINEAR)
            arr = np.asarray(img, dtype=np.float32) / 255.0
        return arr[None, ...]

    def predict_proba(self, image_bytes: bytes) -> np.ndarray:
        if self._session is None and not self.load():
            raise RuntimeError(self._load_error or "model unavailable")
        tensor = self._prepare(image_bytes)
        outputs = self._session.run([self._output_name], {self._input_name: tensor})
        self.touch()
        return np.asarray(outputs[0][0], dtype=np.float64)

    # ------------------------------------------------------------- policy #
    def decide(self, probabilities: np.ndarray) -> dict[str, Any]:
        """Apply the same gates the on-device service used.

        Returns a dict rather than a bare label so the caller can log why a
        decision was made. That matters here: the point of moving inference to
        the server is being able to see what the model actually thought.
        """
        probs = np.asarray(probabilities, dtype=np.float64)
        if probs.size != len(self._labels) or not np.all(np.isfinite(probs)):
            return self._unknown({"reason": "malformed model output"})

        total = float(probs.sum())
        if total > 0 and not np.isclose(total, 1.0, atol=0.02):
            probs = probs / total
        elif total > 0 and np.isclose(total, 1.0, atol=0.02):
            probs = probs / total  # enforce exact normalisation

        ranked = sorted(
            ((self._labels[i], float(probs[i])) for i in range(len(self._labels))),
            key=lambda kv: -kv[1],
        )
        top_label, top_score = ranked[0]

        not_waste_idx = self._labels.index(NOT_WASTE)
        not_waste = float(probs[not_waste_idx])

        waste_idx = [i for i in range(len(self._labels)) if i != not_waste_idx]
        best_i = max(waste_idx, key=lambda i: probs[i])
        best_score = float(probs[best_i])
        others = [float(probs[i]) for i in waste_idx if i != best_i]
        margin = best_score - (max(others) if others else 0.0)

        confident_waste = (
            best_score >= MIN_CATEGORY_PROBABILITY
            and margin >= MIN_CATEGORY_MARGIN
            and not_waste <= MAX_NOT_WASTE_FOR_WASTE
        )
        confident_not_waste = (
            top_label == NOT_WASTE
            and top_score >= MIN_NOT_WASTE_PROBABILITY
            and (top_score - ranked[1][1]) >= MIN_CATEGORY_MARGIN
            and best_score < MIN_CATEGORY_PROBABILITY
        )

        payload = {
            "probabilities": {label: round(float(probs[i]), 6)
                              for i, label in enumerate(self._labels)},
            "top_label": top_label,
            "top_score": round(top_score, 6),
            "not_waste_probability": round(not_waste, 6),
            "best_waste_margin": round(margin, 6),
        }

        if confident_waste:
            category = self._labels[best_i]
            return self._result(category, best_score, payload)
        if confident_not_waste:
            return self._result(NOT_WASTE, top_score, payload)

        payload["reason"] = "below confidence gate"
        return self._unknown(payload)

    @staticmethod
    def _result(category: str, score: float, payload: dict) -> dict[str, Any]:
        return {
            "category": category,
            "display_name": DISPLAY_NAMES.get(category, category),
            "bin": BIN_GUIDANCE.get(category, ""),
            "confidence": round(float(score), 4),
            "is_waste": category not in (NOT_WASTE, UNKNOWN),
            "confident": True,
            **payload,
        }

    def _unknown(self, payload: dict) -> dict[str, Any]:
        return {
            "category": UNKNOWN,
            "display_name": DISPLAY_NAMES[UNKNOWN],
            "bin": "",
            "confidence": round(float(payload.get("top_score", 0.0)), 4),
            "is_waste": False,
            "confident": False,
            "probabilities": payload.get("probabilities", {}),
            "top_label": payload.get("top_label"),
            "top_score": payload.get("top_score", 0.0),
            "not_waste_probability": payload.get("not_waste_probability"),
            "best_waste_margin": payload.get("best_waste_margin"),
            "reason": payload.get("reason", "low confidence"),
        }
