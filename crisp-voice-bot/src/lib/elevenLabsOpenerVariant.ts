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
  const cellIndex = Math.abs(rowNumber) % 4;
  return Math.floor(cellIndex / 2) === 0 ? "direct_reason" : "benefit_hook";
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
        label: "Permission-first help check",
        script: "We help agents with short-sale paperwork and lender calls. Is it worth a quick minute to see if that would be useful on this one?",
      };
    case "direct_reason":
    default:
      return {
        key: "direct_reason",
        label: "Permission-first handling check",
        script: "I was calling about the short-sale paperwork and lender calls. Is it okay if I ask one quick question about that?",
      };
  }
}
