export type ElevenLabsOpenerVariantKey =
  | "direct_reason"
  | "yoni_name"
  | "benefit_hook"
  | "identity_check_short";

export type ElevenLabsOpenerVariant = {
  key: ElevenLabsOpenerVariantKey;
  label: string;
  script: string;
};

type BuildOpenerVariantInput = {
  rowNumber: number;
  firstName?: string;
  assistantName: string;
};

function weightedVariantForRow(rowNumber: number): ElevenLabsOpenerVariantKey {
  return Math.abs(rowNumber) % 2 === 0 ? "direct_reason" : "benefit_hook";
}

export function buildElevenLabsOpenerVariant(input: BuildOpenerVariantInput): ElevenLabsOpenerVariant {
  const key = weightedVariantForRow(input.rowNumber);

  switch (key) {
    case "identity_check_short":
      return {
        key,
        label: "Short identity check control",
        script: input.firstName?.trim()
          ? `Is this ${input.firstName.trim()}?`
          : "Is this the listing agent?",
      };
    case "yoni_name":
      return {
        key,
        label: "Yoni name upfront",
        script: "I'm calling for Yoni Kutler. Are you handling the short sale paperwork and lender calls yourself?",
      };
    case "benefit_hook":
      return {
        key,
        label: "Direct help question",
        script: "We help agents with the short sale paperwork and lender calls. Are you looking for help with that?",
      };
    case "direct_reason":
    default:
      return {
        key: "direct_reason",
        label: "Plain handling question",
        script: "Are you handling the short sale paperwork and lender calls yourself?",
      };
  }
}
