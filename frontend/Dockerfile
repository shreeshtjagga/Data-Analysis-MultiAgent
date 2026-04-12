FROM node:20-alpine

WORKDIR /app

# Install dependencies
COPY package*.json ./
RUN npm install

# Copy source
COPY . .

EXPOSE 5173

# Start Vite dev server (bound to 0.0.0.0 so Docker can expose it)
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0"]
