export class CreateTicketDto {
    title!: string;
    description!: string;
    priority!: "low" | "medium" | "high";
}