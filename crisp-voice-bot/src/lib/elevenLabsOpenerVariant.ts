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

function getGreeting(firstName?: string): string {
  const name = firstName?.trim();
  return name ? `Hi ${name},` : "Hi,";
}

function weightedVariantForRow(rowNumber: number): ElevenLabsOpenerVariantKey {
  const bucket = Math.abs(rowNumber) % 10;

  if (bucket === 0) {
    return "identity_check_short";
  }

  if (bucket <= 3) {
    return "direct_reason";
  }

  if (bucket <= 6) {
    return "yoni_name";
  }

  return "benefit_hook";
}

export function buildElevenLabsOpenerVariant(input: BuildOpenerVariantInput): ElevenLabsOpenerVariant {
  const key = weightedVariantForRow(input.rowNumber);
  const greeting = getGreeting(input.firstName);

  switch (key) {
    case "identity_check_short":
      return {
        key,
        label: "Short identity check control",
        script: input.firstName?.trim() ? `Hey, is this ${input.firstName.trim()}?` : "Hey, is this the listing agent?",
      };
    case "yoni_name":
      return {
        key,
        label: "Yoni name upfront",
        script: `${greeting} ${input.assistantName} calling for Yoni Kutler about your short sale listing. Are you handling the bank side yourself?`,
      };
    case "benefit_hook":
      return {
        key,
        label: "Benefit hook upfront",
        script: `${greeting} this is ${input.assistantName} with Crisp Short Sales. We help agents with lender calls and paperwork on short sales. Are you handling that one yourself?`,
      };
    case "direct_reason":
    default:
      return {
        key: "direct_reason",
        label: "Direct short sale reason",
        script: `${greeting} this is ${input.assistantName} with Crisp Short Sales about your short sale listing. Are you handling the bank side yourself?`,
      };
  }
}
