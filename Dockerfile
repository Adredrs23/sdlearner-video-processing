# Stage 1: Build
FROM mcr.microsoft.com/dotnet/sdk:9.0-preview AS build
WORKDIR /app

COPY *.csproj ./
RUN dotnet restore

COPY . ./
RUN dotnet publish -c Release -o out

# Stage 2: Runtime
FROM mcr.microsoft.com/dotnet/runtime:9.0 AS runtime
WORKDIR /app

COPY --from=build /app/out .

ENTRYPOINT ["dotnet", "VideoProcessorWorker.dll"]
