export type OwnerCallContext = {
  status: "matched" | "multiple_listings" | "ambiguous" | "unknown" | "unavailable";
  agentName?: string;
  listingAddress?: string;
  rowNumber?: number;
};

export function cleanAnnouncementText(value: unknown): string {
  return typeof value === "string" ? value.replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim().slice(0, 700) : "";
}

export function buildReturnCallAnnouncement(context: OwnerCallContext): string {
  const name = cleanAnnouncementText(context.agentName);
  const listing = cleanAnnouncementText(context.listingAddress);
  if (context.status === "matched" && name && listing) {
    return `Crisp return call. Caller number matches agent ${name}. Listing: ${listing}. Press 1 to connect.`;
  }
  if (context.status === "multiple_listings" && name) {
    return `Crisp return call. Caller number matches agent ${name}. Multiple listings on file; property not confirmed. Press 1 to connect.`;
  }
  if (context.status === "ambiguous") {
    return "Crisp return call. Multiple agent records match this number; caller and property not confirmed. Press 1 to connect.";
  }
  return "Crisp return call. Agent name and property could not be identified. Press 1 to connect.";
}

export function buildLiveTransferAnnouncement(context: Pick<OwnerCallContext, "agentName" | "listingAddress">): string {
  const name = cleanAnnouncementText(context.agentName);
  const listing = cleanAnnouncementText(context.listingAddress);
  return `Crisp live transfer${name && name.toLowerCase() !== "unknown" ? ` from agent ${name}` : "; agent name unavailable"}. ` +
    `${listing && listing.toLowerCase() !== "unknown" ? `Listing: ${listing}.` : "Property not identified."} Press 1 to accept.`;
}
