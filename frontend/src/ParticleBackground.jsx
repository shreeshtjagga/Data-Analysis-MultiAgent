import { useEffect, useRef } from 'react';

export default function ParticleBackground({ noExclude = false }) {
  const canvasRef = useRef(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    let animationFrameId;
    
    // Config
    let particles = [];
    const numParticles = noExclude? 35 : 80; 
    const maxDistance = 160;

    const resize = () => {
      canvas.width = window.innerWidth;
      canvas.height = window.innerHeight;
    };
    window.addEventListener('resize', resize);
    window.addEventListener('scroll', resize);
    resize();

    // Exclusion zone logic
    const getExcludeZone = () => {
      if (noExclude) return { xMin: -1, xMax: -1, yMin: -1, yMax: -1 };
      const centerX = canvas.width / 2;
      const centerY = canvas.height / 2;
      const padX = 300; // 600px total width
      const padY = 350; // 700px total height
      return {
        xMin: centerX - padX,
        xMax: centerX + padX,
        yMin: centerY - padY,
        yMax: centerY + padY
      };
    };

    class Particle {
      constructor() {
        this.reset();
      }

      reset() {
        // Spawn outside center exclusion zone if possible
        const zone = getExcludeZone();
        let spawnedInRange = true;
        let attempts = 0;
        
        while(spawnedInRange && attempts < 10) {
          this.x = Math.random() * canvas.width;
          this.y = Math.random() * canvas.height;
          
          if (!(this.x > zone.xMin && this.x < zone.xMax && this.y > zone.yMin && this.y < zone.yMax)) {
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

        const zone = getExcludeZone();
        // If entering exclusion zone, bounce back or loop
        if (this.x > zone.xMin && this.x < zone.xMax && this.y > zone.yMin && this.y < zone.yMax) {
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

    for (let i = 0; i < numParticles; i++) {
        particles.push(new Particle());
    }

    let frameCount = 0;
    const drawCanvas = () => {
      frameCount++;
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      
      for (let i = 0; i < numParticles; i++) {
        particles[i].update();
        particles[i].draw();
        ctx.shadowBlur = 0;

        if (frameCount % 2 === 0) {
          for (let j = i + 1; j < numParticles; j++) {
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
      window.removeEventListener('scroll', resize);
      cancelAnimationFrame(animationFrameId);
    };
  }, [noExclude]);

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
