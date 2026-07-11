# syntax=docker/dockerfile:1.7
# Build context: this component's own directory (self-contained).

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,id=nuget,target=/root/.nuget/packages \
    dotnet restore TransactionService/TransactionService.csproj && \
    dotnet restore TransactionService.Tests/TransactionService.Tests.csproj && \
    dotnet test TransactionService.Tests/TransactionService.Tests.csproj -c Release --no-restore -v minimal && \
    dotnet publish TransactionService/TransactionService.csproj -c Release --no-restore -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0-noble-chiseled
WORKDIR /app
COPY --from=build /app .
ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080
ENTRYPOINT ["dotnet", "TransactionService.dll"]
