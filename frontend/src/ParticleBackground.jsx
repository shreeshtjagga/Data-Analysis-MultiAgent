import { useEffect, useRef } from 'react';

export default function ParticleBackground({
  noExclude = false,
  exclusionSelectors = [],
  exclusionPadding = 12,
}) {
  const canvasRef = useRef(null);
  const noExcludeRef = useRef(noExclude);
  const exclusionSelectorsRef = useRef(exclusionSelectors);
  const exclusionPaddingRef = useRef(exclusionPadding);

  useEffect(() => {
    noExcludeRef.current = noExclude;
  }, [noExclude]);

  useEffect(() => {
    exclusionSelectorsRef.current = exclusionSelectors;
  }, [exclusionSelectors]);

  useEffect(() => {
    exclusionPaddingRef.current = exclusionPadding;
  }, [exclusionPadding]);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    let animationFrameId;
    
    // Config
    let particles = [];
    const maxDistance = 160;

    const getRuntimeConfig = () => ({
      targetParticles: noExcludeRef.current ? 28 : 80,
      drawConnections: !noExcludeRef.current,
    });

    const resize = () => {
      canvas.width = window.innerWidth;
      canvas.height = window.innerHeight;
      refreshExcludeRects();
    };

    let excludeRects = [];

    // Exclusion zone logic
    const getFallbackExcludeZone = () => {
      const centerX = canvas.width / 2;
      const centerY = canvas.height / 2;
      const padX = 300; // 600px total width
      const padY = 350; // 700px total height
      return [{
        xMin: centerX - padX,
        xMax: centerX + padX,
        yMin: centerY - padY,
        yMax: centerY + padY,
      }];
    };

    const refreshExcludeRects = () => {
      if (noExcludeRef.current) {
        excludeRects = [];
        return;
      }

      const selectors = exclusionSelectorsRef.current || [];
      const pad = Number(exclusionPaddingRef.current) || 0;

      if (!selectors.length) {
        excludeRects = getFallbackExcludeZone();
        return;
      }

      const rects = [];
      selectors.forEach((selector) => {
        const nodes = document.querySelectorAll(selector);
        nodes.forEach((node) => {
          const rect = node.getBoundingClientRect();
          if (rect.width <= 0 || rect.height <= 0) return;
          rects.push({
            xMin: Math.max(0, rect.left - pad),
            xMax: Math.min(canvas.width, rect.right + pad),
            yMin: Math.max(0, rect.top - pad),
            yMax: Math.min(canvas.height, rect.bottom + pad),
          });
        });
      });

      excludeRects = rects.length ? rects : getFallbackExcludeZone();
    };

    const isInsideExclude = (x, y) => {
      if (!excludeRects.length) return false;
      for (const rect of excludeRects) {
        if (x > rect.xMin && x < rect.xMax && y > rect.yMin && y < rect.yMax) {
          return true;
        }
      }
      return false;
    };

    window.addEventListener('resize', resize);
    resize();

    refreshExcludeRects();

    class Particle {
      constructor() {
        this.reset();
      }

      reset() {
        // Spawn outside exclusion zone if possible
        let spawnedInRange = true;
        let attempts = 0;
        
        while (spawnedInRange && attempts < 12) {
          this.x = Math.random() * canvas.width;
          this.y = Math.random() * canvas.height;

          if (!isInsideExclude(this.x, this.y)) {
            spawnedInRange = false;
          }
          attempts++;
        }

        this.vx = (Math.random() - 0.5) * 0.8; 
        this.vy = (Math.random() - 0.5) * 0.8;
        this.radius = Math.random() * 3 + 2; 
      }

      update() {
        this.x += this.vx;
        this.y += this.vy;

        // If entering exclusion zone, bounce back or loop
        if (isInsideExclude(this.x, this.y)) {
          // Simplest: reverse velocity
          this.vx *= -1;
          this.vy *= -1;
          // Step back
          this.x += this.vx * 2;
          this.y += this.vy * 2;
        }

        if (this.x < 0 || this.x > canvas.width) this.vx = -this.vx;
        if (this.y < 0 || this.y > canvas.height) this.vy = -this.vy;
      }

      draw() {
        ctx.beginPath();
        ctx.arc(this.x, this.y, this.radius, 0, Math.PI * 2);
        ctx.fillStyle = 'rgba(99, 102, 241, 0.75)';
        ctx.fill();
        
        ctx.shadowBlur = 12;
        ctx.shadowColor = 'rgba(99, 102, 241, 0.9)';
      }
    }

    for (let i = 0; i < getRuntimeConfig().targetParticles; i++) {
        particles.push(new Particle());
    }

    let frameCount = 0;
    const drawCanvas = () => {
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      const { targetParticles, drawConnections } = getRuntimeConfig();
      frameCount += 1;

      // Refresh dynamic panel/tab exclusion rectangles periodically
      if (frameCount % 30 === 0) {
        refreshExcludeRects();
      }

      if (particles.length < targetParticles) {
        for (let i = particles.length; i < targetParticles; i++) {
          particles.push(new Particle());
        }
      } else if (particles.length > targetParticles) {
        particles.length = targetParticles;
      }
      
      for (let i = 0; i < particles.length; i++) {
        particles[i].update();
        particles[i].draw();
        ctx.shadowBlur = 0;

        if (drawConnections) {
          for (let j = i + 1; j < particles.length; j++) {
            if (isInsideExclude(particles[i].x, particles[i].y) || isInsideExclude(particles[j].x, particles[j].y)) {
              continue;
            }
            const dx = particles[i].x - particles[j].x;
            const dy = particles[i].y - particles[j].y;
            const dist = Math.sqrt(dx * dx + dy * dy);
            if (dist < maxDistance) {
              ctx.beginPath();
              ctx.moveTo(particles[i].x, particles[i].y);
              ctx.lineTo(particles[j].x, particles[j].y);
              const alpha = 1 - (dist / maxDistance);
              ctx.strokeStyle = `rgba(99, 102, 241, ${alpha * 0.35})`;
              ctx.lineWidth = 1.2;
              ctx.stroke();
            }
          }
        }
      }
      animationFrameId = requestAnimationFrame(drawCanvas);
    };

    drawCanvas();

    return () => {
      window.removeEventListener('resize', resize);
      cancelAnimationFrame(animationFrameId);
    };
  }, []);

  return (
    <canvas 
      ref={canvasRef} 
      style={{
        position: 'fixed',
        top: 0,
        left: 0,
        width: '100%',
        height: '100%',
        zIndex: 0,
        pointerEvents: 'none',
        background: 'linear-gradient(135deg, #060912 0%, #0d1220 100%)' 
      }} 
    />
  );
}
