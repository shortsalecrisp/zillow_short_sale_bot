export type OwnerCallContext = {
  status: "matched" | "multiple_listings" | "ambiguous" | "unknown" | "unavailable";
  agentName?: string;
  agentFirstName?: string;
  listingAddress?: string;
  rowNumber?: number;
};

export function cleanAnnouncementText(value: unknown): string {
  return typeof value === "string" ? value.replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim().slice(0, 700) : "";
}

export function buildReturnCallAnnouncement(context: OwnerCallContext): string {
  const name = cleanAnnouncementText(context.agentFirstName ?? cleanAnnouncementText(context.agentName).split(" ")[0]);
  if (context.status === "matched") {
    const row = Number.isSafeInteger(context.rowNumber) && context.rowNumber! >= 2 ? `Row ${context.rowNumber}.` : "Row unavailable.";
    return `${name || "Agent name unavailable"}. ${row} Press 1 to connect.`;
  }
  if (context.status === "multiple_listings") {
    return `${name ? `${name}. ` : ""}Multiple matching rows. Press 1 to connect.`;
  }
  if (context.status === "ambiguous") {
    return "Multiple agents match. Row not confirmed. Press 1 to connect.";
  }
  return "Agent name and row unavailable. Press 1 to connect.";
}

export function buildLiveTransferAnnouncement(context: Pick<OwnerCallContext, "agentName" | "listingAddress">): string {
  const name = cleanAnnouncementText(context.agentName);
  const listing = cleanAnnouncementText(context.listingAddress);
  return `Crisp live transfer${name && name.toLowerCase() !== "unknown" ? ` from agent ${name}` : "; agent name unavailable"}. ` +
    `${listing && listing.toLowerCase() !== "unknown" ? `Listing: ${listing}.` : "Property not identified."} Press 1 to accept.`;
}
